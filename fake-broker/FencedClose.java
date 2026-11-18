import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * KAFKA-17263: a static member's KafkaConsumer.close() can block forever (classic group protocol).
 *
 * 1. A static member (group.instance.id) gets FENCED_INSTANCE_ID in a heartbeat response, as when another consumer
 *    joins with the same group.instance.id. Its heartbeat thread records the failure, disables itself and parks in an
 *    untimed wait() on the coordinator. The application's next poll() throws FencedInstanceIdException, and
 *    AbstractCoordinator.pollHeartbeat drops the thread (heartbeatThread = null) without closing it. The application
 *    catches the exception and keeps polling: a new heartbeat thread starts, the member rejoins and heartbeats again.
 *    The old thread stays parked for the life of the process.
 * 2. Later the group rebalances (here the broker answers a heartbeat with REBALANCE_IN_PROGRESS and holds the JoinGroup
 *    response, as a broker does while it waits for the other members). The current heartbeat thread disables itself
 *    and parks in wait() on the same monitor, behind the old one.
 * 3. The application calls close() while the rebalance is in progress. closeHeartbeatThread() sets the current thread's closed
 *    flag, calls notify() and join()s it with no timeout. notify() wakes one waiter, and HotSpot picks the one that
 *    has waited longest: the old thread, which waits again. The current thread never wakes, and close() never
 *    returns, whatever its timeout.
 *
 * The broker below is a minimal one-node broker on 127.0.0.1, written with kafka-clients' own message classes.
 *
 *   java FencedClose [fences=N] [close=MS]     default: 1 fence, close(Duration.ofSeconds(5)); fences=0: no fence
 */
public class FencedClose {
    static final String HB = "kafka-coordinator-heartbeat-thread";

    public static void main(String[] args) throws Exception {
        int fences = 1;
        long closeMs = 5000;
        for (String x : args) {
            if (x.startsWith("fences=")) fences = Integer.parseInt(x.substring(7));
            else if (x.startsWith("close=")) closeMs = Long.parseLong(x.substring(6));
        }

        Broker broker = new Broker();
        broker.createTopic("t");
        broker.fencesLeft = fences;
        CountDownLatch rebalanceJoinSent = new CountDownLatch(1);
        CountDownLatch never = new CountDownLatch(1);
        broker.gates = key -> {
            if (key == ApiKeys.JOIN_GROUP && broker.rebalancing) {
                rebalanceJoinSent.countDown();
                return never;
            }
            return null;
        };
        broker.start();

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:" + broker.port);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "g");
        p.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "instance-1");
        p.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.subscribe(List.of("t"));

        // The application: poll; on FencedInstanceIdException, log it and keep polling.
        int caught = 0;
        long end = System.currentTimeMillis() + 20_000;
        long settledAt = Long.MAX_VALUE;
        while (System.currentTimeMillis() < end) {
            try {
                consumer.poll(Duration.ofMillis(50));
            } catch (FencedInstanceIdException e) {
                caught++;
                settledAt = Long.MAX_VALUE;
            }
            boolean rejoined = caught == fences && !consumer.assignment().isEmpty();
            if (rejoined && settledAt == Long.MAX_VALUE) settledAt = System.currentTimeMillis() + 2000;
            if (!broker.rebalancing && System.currentTimeMillis() >= settledAt) {
                broker.rebalancing = true; // 2 s of normal heartbeats later, the group rebalances
            }
            if (rebalanceJoinSent.getCount() == 0) break;
        }
        // The rebalance is in progress (the broker is "waiting for other members"); the application keeps polling
        // for a moment, then shuts down.
        for (long stop = System.currentTimeMillis() + 500; System.currentTimeMillis() < stop; ) {
            consumer.poll(Duration.ofMillis(50));
        }
        System.out.println("fenced heartbeats " + broker.fenced + ", FencedInstanceIdException caught " + caught
                + "; rejoined; then a rebalance: its JoinGroup " + (rebalanceJoinSent.getCount() == 0 ? "in flight" : "NOT SENT"));
        Thread current = currentHeartbeatThread(consumer);
        System.out.println("heartbeat threads before close():");
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().startsWith(HB) && t.isAlive()) {
                System.out.println("    #" + t.getId() + " " + t.getState()
                        + (t == current ? "  the consumer's current heartbeat thread" : "  dropped by pollHeartbeat, not closed"));
            }
        }

        Duration timeout = Duration.ofMillis(closeMs);
        Thread closer = new Thread(() -> consumer.close(timeout), "closer");
        closer.setDaemon(true);
        long t0 = System.currentTimeMillis();
        closer.start();
        closer.join(closeMs + 10_000);
        boolean hung = closer.isAlive();
        System.out.println("close(" + timeout + ") " + (hung ? "HUNG (still running " + (closeMs + 10_000) / 1000 + " s later)"
                : "returned in " + (System.currentTimeMillis() - t0) + " ms"));
        if (hung) {
            for (StackTraceElement e : closer.getStackTrace()) {
                if (e.getClassName().startsWith("org.apache.kafka") || e.getMethodName().equals("join"))
                    System.out.println("    at " + e);
            }
        }
        Thread.sleep(200);
        long left = Thread.getAllStackTraces().keySet().stream().filter(t -> t.getName().startsWith(HB) && t.isAlive()).count();
        System.out.println("heartbeat threads alive after close(): " + left);
        boolean ok = !hung && left == 0;
        System.out.println(ok ? "RESULT: OK" : "RESULT: BUG");
        System.exit(ok ? 0 : 1);
    }

    /** For the report only: which thread is AbstractCoordinator.heartbeatThread (read reflectively; not changed). */
    static Thread currentHeartbeatThread(KafkaConsumer<?, ?> consumer) {
        try {
            Object o = consumer;
            for (String name : new String[] {"delegate", "coordinator", "heartbeatThread"}) {
                Field f = null;
                for (Class<?> c = o.getClass(); f == null && c != null; c = c.getSuperclass()) {
                    try {
                        f = c.getDeclaredField(name);
                    } catch (NoSuchFieldException e) {
                        // superclass
                    }
                }
                if (f == null) {
                    if (name.equals("delegate")) continue; // 3.6 and earlier: no delegate
                    return null;
                }
                f.setAccessible(true);
                o = f.get(o);
                if (o == null) return null;
            }
            return (Thread) o;
        } catch (ReflectiveOperationException | RuntimeException e) {
            return null;
        }
    }

    /**
     * A one-node broker for one static member (classic protocol): one partition per topic, no records, and a group
     * whose only member is always the leader. Each connection answers its requests in order; {@code gates} can hold a
     * connection's response (and everything after it on that connection) until a latch opens.
     */
    static final class Broker {
        final ServerSocket server;
        final int port;
        final Map<String, Uuid> topicIds = new LinkedHashMap<>();
        final Map<TopicPartition, Long> committed = new HashMap<>();
        int generation;
        int memberSeq;
        String memberId = "";
        byte[] assignment = new byte[0];
        volatile Function<ApiKeys, CountDownLatch> gates;
        int fencesLeft;
        volatile int fenced;
        volatile boolean rebalancing;
        final Object emptyRecords;

        Broker() throws Exception {
            server = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
            port = server.getLocalPort();
            Class<?> mr;
            try {
                mr = Class.forName("org.apache.kafka.common.record.internal.MemoryRecords"); // 4.2 and later
            } catch (ClassNotFoundException e) {
                mr = Class.forName("org.apache.kafka.common.record.MemoryRecords");
            }
            emptyRecords = mr.getField("EMPTY").get(null);
        }

        synchronized void createTopic(String name) {
            topicIds.put(name, new Uuid(1, topicIds.size() + 1));
        }

        void start() {
            Thread t = new Thread(() -> {
                while (true) {
                    try {
                        Socket s = server.accept();
                        Thread c = new Thread(() -> serve(s), "broker-connection");
                        c.setDaemon(true);
                        c.start();
                    } catch (IOException e) {
                        return;
                    }
                }
            }, "broker-accept");
            t.setDaemon(true);
            t.start();
        }

        void serve(Socket s) {
            try (s) {
                DataInputStream in = new DataInputStream(s.getInputStream());
                OutputStream out = s.getOutputStream();
                while (true) {
                    byte[] bytes;
                    try {
                        bytes = new byte[in.readInt()];
                    } catch (EOFException e) {
                        return;
                    }
                    in.readFully(bytes);
                    ByteBuffer buf = ByteBuffer.wrap(bytes);
                    RequestHeader header = RequestHeader.parse(buf);
                    ApiMessage request = parseRequest(header, buf);
                    ApiMessage response = handle(header.apiKey(), request);
                    Function<ApiKeys, CountDownLatch> g = gates;
                    CountDownLatch gate = g == null ? null : g.apply(header.apiKey());
                    if (gate != null) gate.await();
                    out.write(frame(header, response));
                    out.flush();
                }
            } catch (IOException | InterruptedException e) {
                // connection closed
            }
        }

        /** AbstractRequest.parseRequest takes a ByteBufferAccessor in 4.x and a ByteBuffer in 3.x. */
        static ApiMessage parseRequest(RequestHeader header, ByteBuffer buf) {
            try {
                for (java.lang.reflect.Method m : AbstractRequest.class.getMethods()) {
                    if (m.getName().equals("parseRequest") && m.getParameterCount() == 3) {
                        Object in = m.getParameterTypes()[2] == ByteBuffer.class ? buf : new ByteBufferAccessor(buf);
                        Object r = m.invoke(null, header.apiKey(), header.apiVersion(), in); // RequestAndSize
                        return ((AbstractRequest) r.getClass().getField("request").get(r)).data();
                    }
                }
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
            throw new IllegalStateException("no AbstractRequest.parseRequest");
        }

        static byte[] frame(RequestHeader header, ApiMessage response) {
            ObjectSerializationCache cache = new ObjectSerializationCache();
            ResponseHeader rh = header.toResponseHeader();
            int size = rh.data().size(cache, rh.headerVersion()) + response.size(cache, header.apiVersion());
            ByteBuffer buf = ByteBuffer.allocate(4 + size);
            buf.putInt(size);
            ByteBufferAccessor acc = new ByteBufferAccessor(buf);
            rh.data().write(acc, cache, rh.headerVersion());
            response.write(acc, cache, header.apiVersion());
            return buf.array();
        }

        ApiMessage handle(ApiKeys key, ApiMessage r) throws InterruptedException {
            switch (key) {
                case API_VERSIONS: return apiVersions();
                case METADATA: return metadata((MetadataRequestData) r);
                case FIND_COORDINATOR: return findCoordinator((FindCoordinatorRequestData) r);
                case JOIN_GROUP: return join((JoinGroupRequestData) r);
                case SYNC_GROUP: return sync((SyncGroupRequestData) r);
                case HEARTBEAT: return heartbeat((HeartbeatRequestData) r);
                case LEAVE_GROUP: return leave((LeaveGroupRequestData) r);
                case OFFSET_COMMIT: return commit((OffsetCommitRequestData) r);
                case OFFSET_FETCH: return offsetFetch((OffsetFetchRequestData) r);
                case LIST_OFFSETS: return listOffsets((ListOffsetsRequestData) r);
                case FETCH: return fetch((FetchRequestData) r);
                default: throw new IllegalStateException("unsupported " + key);
            }
        }

        static ApiVersionsResponseData apiVersions() {
            ApiVersionsResponseData d = new ApiVersionsResponseData();
            for (ApiKeys k : new ApiKeys[] {ApiKeys.API_VERSIONS, ApiKeys.METADATA, ApiKeys.FIND_COORDINATOR,
                    ApiKeys.JOIN_GROUP, ApiKeys.SYNC_GROUP, ApiKeys.HEARTBEAT, ApiKeys.LEAVE_GROUP,
                    ApiKeys.OFFSET_COMMIT, ApiKeys.OFFSET_FETCH, ApiKeys.LIST_OFFSETS, ApiKeys.FETCH}) {
                d.apiKeys().add(new ApiVersionsResponseData.ApiVersion().setApiKey(k.id)
                        .setMinVersion(k.oldestVersion()).setMaxVersion(k.latestVersion()));
            }
            return d;
        }

        synchronized MetadataResponseData metadata(MetadataRequestData r) {
            MetadataResponseData d = new MetadataResponseData().setClusterId("fake").setControllerId(0);
            d.brokers().add(new MetadataResponseData.MetadataResponseBroker().setNodeId(0).setHost("127.0.0.1").setPort(port));
            List<String> names = new ArrayList<>();
            if (r.topics() == null) {
                names.addAll(topicIds.keySet());
            } else {
                for (MetadataRequestData.MetadataRequestTopic t : r.topics()) names.add(t.name() != null ? t.name() : nameOf(t.topicId()));
            }
            for (String name : names) {
                MetadataResponseData.MetadataResponseTopic mt = new MetadataResponseData.MetadataResponseTopic().setName(name);
                if (!topicIds.containsKey(name)) {
                    mt.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
                } else {
                    mt.setTopicId(topicIds.get(name));
                    mt.partitions().add(new MetadataResponseData.MetadataResponsePartition().setPartitionIndex(0)
                            .setLeaderId(0).setLeaderEpoch(0).setReplicaNodes(List.of(0)).setIsrNodes(List.of(0)));
                }
                d.topics().add(mt);
            }
            return d;
        }

        String nameOf(Uuid id) {
            for (Map.Entry<String, Uuid> e : topicIds.entrySet()) {
                if (e.getValue().equals(id)) return e.getKey();
            }
            return null;
        }

        FindCoordinatorResponseData findCoordinator(FindCoordinatorRequestData r) {
            FindCoordinatorResponseData d = new FindCoordinatorResponseData();
            for (String k : r.coordinatorKeys()) {
                d.coordinators().add(new FindCoordinatorResponseData.Coordinator().setKey(k).setNodeId(0)
                        .setHost("127.0.0.1").setPort(port));
            }
            return r.coordinatorKeys().isEmpty() ? d.setNodeId(0).setHost("127.0.0.1").setPort(port) : d;
        }

        /** The only member: every join completes the rebalance at once. A static member gets a new member id. */
        synchronized JoinGroupResponseData join(JoinGroupRequestData r) {
            memberId = "instance-1-member-" + (++memberSeq);
            generation++;
            JoinGroupRequestData.JoinGroupRequestProtocol protocol = r.protocols().iterator().next();
            JoinGroupResponseData d = new JoinGroupResponseData().setGenerationId(generation).setProtocolType(r.protocolType())
                    .setProtocolName(protocol.name()).setLeader(memberId).setMemberId(memberId);
            d.members().add(new JoinGroupResponseData.JoinGroupResponseMember().setMemberId(memberId)
                    .setGroupInstanceId(r.groupInstanceId()).setMetadata(protocol.metadata()));
            return d;
        }

        synchronized SyncGroupResponseData sync(SyncGroupRequestData r) {
            r.assignments().forEach(x -> assignment = x.assignment());
            return new SyncGroupResponseData().setProtocolType(r.protocolType()).setProtocolName(r.protocolName())
                    .setAssignment(assignment);
        }

        /**
         * FENCED_INSTANCE_ID for the first {@code fencesLeft} heartbeats, as if another consumer had joined with this
         * group.instance.id; REBALANCE_IN_PROGRESS once {@code rebalancing} is set, as if another member had joined.
         */
        synchronized HeartbeatResponseData heartbeat(HeartbeatRequestData r) {
            Errors e;
            if (fencesLeft > 0) {
                fencesLeft--;
                fenced++;
                e = Errors.FENCED_INSTANCE_ID;
            } else if (rebalancing) {
                e = Errors.REBALANCE_IN_PROGRESS;
            } else {
                e = r.generationId() == generation ? Errors.NONE : Errors.ILLEGAL_GENERATION;
            }
            return new HeartbeatResponseData().setErrorCode(e.code());
        }

        LeaveGroupResponseData leave(LeaveGroupRequestData r) {
            LeaveGroupResponseData d = new LeaveGroupResponseData();
            r.members().forEach(x -> d.members().add(new LeaveGroupResponseData.MemberResponse()
                    .setMemberId(x.memberId()).setGroupInstanceId(x.groupInstanceId())));
            return d;
        }

        synchronized OffsetCommitResponseData commit(OffsetCommitRequestData r) {
            OffsetCommitResponseData d = new OffsetCommitResponseData();
            for (OffsetCommitRequestData.OffsetCommitRequestTopic t : r.topics()) {
                OffsetCommitResponseData.OffsetCommitResponseTopic rt = new OffsetCommitResponseData.OffsetCommitResponseTopic().setName(t.name());
                for (OffsetCommitRequestData.OffsetCommitRequestPartition p : t.partitions()) {
                    rt.partitions().add(new OffsetCommitResponseData.OffsetCommitResponsePartition().setPartitionIndex(p.partitionIndex()));
                }
                d.topics().add(rt);
            }
            return d;
        }

        synchronized OffsetFetchResponseData offsetFetch(OffsetFetchRequestData r) {
            OffsetFetchResponseData d = new OffsetFetchResponseData();
            if (r.groups().isEmpty()) { // versions before 8
                for (OffsetFetchRequestData.OffsetFetchRequestTopic t : r.topics()) {
                    OffsetFetchResponseData.OffsetFetchResponseTopic rt = new OffsetFetchResponseData.OffsetFetchResponseTopic().setName(t.name());
                    for (int p : t.partitionIndexes()) {
                        rt.partitions().add(new OffsetFetchResponseData.OffsetFetchResponsePartition().setPartitionIndex(p)
                                .setCommittedOffset(-1L).setMetadata(""));
                    }
                    d.topics().add(rt);
                }
                return d;
            }
            for (OffsetFetchRequestData.OffsetFetchRequestGroup rg : r.groups()) {
                OffsetFetchResponseData.OffsetFetchResponseGroup g = new OffsetFetchResponseData.OffsetFetchResponseGroup().setGroupId(rg.groupId());
                for (OffsetFetchRequestData.OffsetFetchRequestTopics t : rg.topics()) {
                    OffsetFetchResponseData.OffsetFetchResponseTopics rt = new OffsetFetchResponseData.OffsetFetchResponseTopics().setName(t.name());
                    copyTopicId(t, rt);
                    for (int p : t.partitionIndexes()) {
                        rt.partitions().add(new OffsetFetchResponseData.OffsetFetchResponsePartitions().setPartitionIndex(p)
                                .setCommittedOffset(-1L).setMetadata(""));
                    }
                    g.topics().add(rt);
                }
                d.groups().add(g);
            }
            return d;
        }

        /** OffsetFetch v10 (4.1+) carries topic ids; older message classes have no such field. */
        static void copyTopicId(Object from, Object to) {
            try {
                Object id = from.getClass().getMethod("topicId").invoke(from);
                to.getClass().getMethod("setTopicId", Uuid.class).invoke(to, id);
            } catch (ReflectiveOperationException e) {
                // no topic ids in this version
            }
        }

        synchronized ListOffsetsResponseData listOffsets(ListOffsetsRequestData r) {
            ListOffsetsResponseData d = new ListOffsetsResponseData();
            for (ListOffsetsRequestData.ListOffsetsTopic t : r.topics()) {
                ListOffsetsResponseData.ListOffsetsTopicResponse rt = new ListOffsetsResponseData.ListOffsetsTopicResponse().setName(t.name());
                for (ListOffsetsRequestData.ListOffsetsPartition p : t.partitions()) {
                    rt.partitions().add(new ListOffsetsResponseData.ListOffsetsPartitionResponse().setPartitionIndex(p.partitionIndex())
                            .setOffset(0).setTimestamp(-1L).setLeaderEpoch(0));
                }
                d.topics().add(rt);
            }
            return d;
        }

        /** MemoryRecords moved to org.apache.kafka.common.record.internal in 4.2; set it either way. */
        static void setRecords(FetchResponseData.PartitionData pd, Object records) {
            try {
                for (java.lang.reflect.Method m : pd.getClass().getMethods()) {
                    if (m.getName().equals("setRecords") && m.getParameterCount() == 1) {
                        m.invoke(pd, records);
                        return;
                    }
                }
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
        }

        /** The log is empty: wait out maxWaitMs, as a broker does, and answer with no records. */
        FetchResponseData fetch(FetchRequestData r) throws InterruptedException {
            Thread.sleep(Math.min(r.maxWaitMs(), 500));
            FetchResponseData d = new FetchResponseData();
            for (FetchRequestData.FetchTopic t : r.topics()) {
                FetchResponseData.FetchableTopicResponse rt = new FetchResponseData.FetchableTopicResponse()
                        .setTopic(t.topic()).setTopicId(t.topicId());
                for (FetchRequestData.FetchPartition p : t.partitions()) {
                    FetchResponseData.PartitionData pd = new FetchResponseData.PartitionData().setPartitionIndex(p.partition())
                            .setHighWatermark(0).setLastStableOffset(0).setLogStartOffset(0);
                    setRecords(pd, emptyRecords);
                    rt.partitions().add(pd);
                }
                d.responses().add(rt);
            }
            return d;
        }
    }
}
