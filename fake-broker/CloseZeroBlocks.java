import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
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
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * KAFKA-17734: KafkaConsumer.close(Duration.ZERO) blocks forever (classic group protocol).
 *
 * A consumer that has joined its group re-subscribes, so it rejoins. Its JoinGroup is in flight when the application
 * calls close(Duration.ZERO). A poll inside close() reads the JoinGroup response; the member is the leader, so
 * onLeaderElected -> updateGroupSubscription -> client.ensureFreshMetadata(time.timer(Long.MAX_VALUE)). The metadata
 * is stale and the broker no longer answers Metadata (as if partitioned), so that wait never ends.
 *
 * The broker below is a minimal one-node, one-member Kafka broker on 127.0.0.1, written with kafka-clients' own
 * message classes. It holds the rejoin's JoinGroup response until just before close(), and stops answering Metadata.
 *
 *   java CloseZeroBlocks eager         default assignors (eager RangeAssignor first), auto-commit on
 *   java CloseZeroBlocks cooperative   CooperativeStickyAssignor, auto-commit on, records consumed first
 *   ... close=MS                       close(Duration.ofMillis(MS)) instead of close(Duration.ZERO)
 */
public class CloseZeroBlocks {
    public static void main(String[] args) throws Exception {
        List<String> a = List.of(args);
        boolean cooperative = a.contains("cooperative");
        long closeMs = 0;
        for (String x : a) {
            if (x.startsWith("close=")) closeMs = Long.parseLong(x.substring(6));
        }

        Broker broker = new Broker();
        broker.createTopic("t");
        broker.createTopic("u");
        if (cooperative) {
            for (int i = 0; i < 5; i++) broker.append("t", "v" + i);
        }
        AtomicInteger joins = new AtomicInteger();
        CountDownLatch rejoinSent = new CountDownLatch(1);
        CountDownLatch rejoinResponse = new CountDownLatch(1);
        CountDownLatch never = new CountDownLatch(1);
        boolean[] metadataDown = {false};
        broker.gates = key -> {
            if (key == ApiKeys.JOIN_GROUP && joins.incrementAndGet() >= 2) {
                rejoinSent.countDown();
                return rejoinResponse;
            }
            return key == ApiKeys.METADATA && metadataDown[0] ? never : null;
        };
        broker.start();

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:" + broker.port);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "g");
        p.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic");
        p.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "100");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        if (cooperative) {
            p.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        }
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);

        consumer.subscribe(List.of("t"));
        long end = System.currentTimeMillis() + 10_000;
        int got = 0;
        while ((consumer.assignment().isEmpty() || (cooperative && got < 5)) && System.currentTimeMillis() < end) {
            got += consumer.poll(Duration.ofMillis(50)).count();
        }
        System.out.println("joined, assignment " + consumer.assignment() + ", consumed " + got + " records");

        consumer.subscribe(List.of("t", "u")); // a new subscription: the member rejoins
        while (rejoinSent.getCount() > 0 && System.currentTimeMillis() < end) consumer.poll(Duration.ofMillis(50));
        System.out.println("rejoin's JoinGroup sent: " + (rejoinSent.getCount() == 0));
        metadataDown[0] = true;     // the broker stops answering Metadata
        Thread.sleep(300);          // the metadata goes stale (metadata.max.age.ms 100)
        rejoinResponse.countDown(); // the JoinGroup response arrives...
        Thread.sleep(100);          // ...and waits in the socket, unread

        Duration timeout = Duration.ofMillis(closeMs);
        Thread closer = new Thread(() -> consumer.close(timeout), "closer");
        closer.setDaemon(true);
        long t0 = System.currentTimeMillis();
        closer.start();
        closer.join(15_000);
        boolean hung = closer.isAlive();
        System.out.println("close(" + timeout + ") " + (hung ? "HUNG (still running after 15 s)"
                : "returned in " + (System.currentTimeMillis() - t0) + " ms"));
        if (hung) {
            for (StackTraceElement e : closer.getStackTrace()) {
                if (e.getClassName().startsWith("org.apache.kafka")) System.out.println("    at " + e);
            }
        }
        System.out.println(hung ? "RESULT: BUG" : "RESULT: OK");
        System.exit(hung ? 1 : 0);
    }

    /**
     * A one-node broker for one consumer (classic protocol): one partition per topic, logs in memory, and a group with
     * a single member, which is therefore always the leader. Each connection answers its requests in order;
     * {@code gates} can hold a connection's response (and everything after it on that connection) until a latch opens.
     */
    static final class Broker {
        static final Set<ApiKeys> SUPPORTED = Set.of(ApiKeys.API_VERSIONS, ApiKeys.METADATA,
                ApiKeys.FIND_COORDINATOR, ApiKeys.JOIN_GROUP, ApiKeys.SYNC_GROUP, ApiKeys.HEARTBEAT,
                ApiKeys.LEAVE_GROUP, ApiKeys.OFFSET_COMMIT, ApiKeys.OFFSET_FETCH, ApiKeys.LIST_OFFSETS, ApiKeys.FETCH);

        final ServerSocket server;
        final int port;
        final Map<String, Uuid> topicIds = new LinkedHashMap<>();
        final Map<String, List<SimpleRecord>> logs = new HashMap<>();
        final Map<TopicPartition, Long> committed = new HashMap<>();
        int generation;
        byte[] assignment = new byte[0];
        volatile Function<ApiKeys, CountDownLatch> gates;

        Broker() throws IOException {
            server = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
            port = server.getLocalPort();
        }

        synchronized void createTopic(String name) {
            topicIds.put(name, new Uuid(1, topicIds.size() + 1));
            logs.put(name, new ArrayList<>());
        }

        synchronized void append(String topic, String value) {
            logs.get(topic).add(new SimpleRecord(System.currentTimeMillis(), null, value.getBytes()));
            notifyAll();
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
                    ApiMessage request = AbstractRequest.parseRequest(header.apiKey(), header.apiVersion(),
                            new ByteBufferAccessor(buf)).request.data();
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
            return switch (key) {
                case API_VERSIONS -> new ApiVersionsResponseData()
                        .setApiKeys(ApiVersionsResponse.collectApis(ApiMessageType.ListenerType.BROKER, SUPPORTED, false));
                case METADATA -> metadata((MetadataRequestData) r);
                case FIND_COORDINATOR -> findCoordinator((FindCoordinatorRequestData) r);
                case JOIN_GROUP -> join((JoinGroupRequestData) r);
                case SYNC_GROUP -> sync((SyncGroupRequestData) r);
                case HEARTBEAT -> heartbeat((HeartbeatRequestData) r);
                case LEAVE_GROUP -> leave((LeaveGroupRequestData) r);
                case OFFSET_COMMIT -> commit((OffsetCommitRequestData) r);
                case OFFSET_FETCH -> offsetFetch((OffsetFetchRequestData) r);
                case LIST_OFFSETS -> listOffsets((ListOffsetsRequestData) r);
                case FETCH -> fetch((FetchRequestData) r);
                default -> throw new IllegalStateException("unsupported " + key);
            };
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

        /** The only member: every join completes the rebalance at once, with this member as leader. */
        synchronized JoinGroupResponseData join(JoinGroupRequestData r) {
            String member = r.memberId().isEmpty() ? "member-1" : r.memberId();
            generation++;
            JoinGroupRequestData.JoinGroupRequestProtocol protocol = r.protocols().iterator().next();
            JoinGroupResponseData d = new JoinGroupResponseData().setGenerationId(generation).setProtocolType(r.protocolType())
                    .setProtocolName(protocol.name()).setLeader(member).setMemberId(member);
            d.members().add(new JoinGroupResponseData.JoinGroupResponseMember().setMemberId(member).setMetadata(protocol.metadata()));
            return d;
        }

        synchronized SyncGroupResponseData sync(SyncGroupRequestData r) {
            r.assignments().forEach(x -> assignment = x.assignment());
            return new SyncGroupResponseData().setProtocolType(r.protocolType()).setProtocolName(r.protocolName())
                    .setAssignment(assignment);
        }

        synchronized HeartbeatResponseData heartbeat(HeartbeatRequestData r) {
            Errors e = r.generationId() == generation ? Errors.NONE : Errors.ILLEGAL_GENERATION;
            return new HeartbeatResponseData().setErrorCode(e.code());
        }

        LeaveGroupResponseData leave(LeaveGroupRequestData r) {
            LeaveGroupResponseData d = new LeaveGroupResponseData();
            r.members().forEach(x -> d.members().add(new LeaveGroupResponseData.MemberResponse().setMemberId(x.memberId())));
            return d;
        }

        synchronized OffsetCommitResponseData commit(OffsetCommitRequestData r) {
            OffsetCommitResponseData d = new OffsetCommitResponseData();
            for (OffsetCommitRequestData.OffsetCommitRequestTopic t : r.topics()) {
                String name = t.name() != null && !t.name().isEmpty() ? t.name() : nameOf(t.topicId());
                OffsetCommitResponseData.OffsetCommitResponseTopic rt = new OffsetCommitResponseData.OffsetCommitResponseTopic()
                        .setName(t.name()).setTopicId(t.topicId());
                for (OffsetCommitRequestData.OffsetCommitRequestPartition p : t.partitions()) {
                    committed.put(new TopicPartition(name, p.partitionIndex()), p.committedOffset());
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
                                .setCommittedOffset(committed.getOrDefault(new TopicPartition(t.name(), p), -1L)).setMetadata(""));
                    }
                    d.topics().add(rt);
                }
                return d;
            }
            for (OffsetFetchRequestData.OffsetFetchRequestGroup rg : r.groups()) {
                OffsetFetchResponseData.OffsetFetchResponseGroup g = new OffsetFetchResponseData.OffsetFetchResponseGroup().setGroupId(rg.groupId());
                for (OffsetFetchRequestData.OffsetFetchRequestTopics t : rg.topics()) {
                    String name = t.name() != null && !t.name().isEmpty() ? t.name() : nameOf(t.topicId());
                    OffsetFetchResponseData.OffsetFetchResponseTopics rt = new OffsetFetchResponseData.OffsetFetchResponseTopics()
                            .setName(t.name()).setTopicId(t.topicId());
                    for (int p : t.partitionIndexes()) {
                        rt.partitions().add(new OffsetFetchResponseData.OffsetFetchResponsePartitions().setPartitionIndex(p)
                                .setCommittedOffset(committed.getOrDefault(new TopicPartition(name, p), -1L)).setMetadata(""));
                    }
                    g.topics().add(rt);
                }
                d.groups().add(g);
            }
            return d;
        }

        synchronized ListOffsetsResponseData listOffsets(ListOffsetsRequestData r) {
            ListOffsetsResponseData d = new ListOffsetsResponseData();
            for (ListOffsetsRequestData.ListOffsetsTopic t : r.topics()) {
                ListOffsetsResponseData.ListOffsetsTopicResponse rt = new ListOffsetsResponseData.ListOffsetsTopicResponse().setName(t.name());
                for (ListOffsetsRequestData.ListOffsetsPartition p : t.partitions()) {
                    long offset = p.timestamp() == -2L ? 0 : logs.get(t.name()).size(); // EARLIEST, else LATEST
                    rt.partitions().add(new ListOffsetsResponseData.ListOffsetsPartitionResponse().setPartitionIndex(p.partitionIndex())
                            .setOffset(offset).setTimestamp(-1L).setLeaderEpoch(0));
                }
                d.topics().add(rt);
            }
            return d;
        }

        /** Waits up to maxWaitMs for data, as a broker does. */
        synchronized FetchResponseData fetch(FetchRequestData r) throws InterruptedException {
            long deadline = System.currentTimeMillis() + r.maxWaitMs();
            while (true) {
                FetchResponseData d = new FetchResponseData();
                boolean any = false;
                for (FetchRequestData.FetchTopic t : r.topics()) {
                    String name = t.topic() != null && !t.topic().isEmpty() ? t.topic() : nameOf(t.topicId());
                    List<SimpleRecord> log = logs.get(name);
                    FetchResponseData.FetchableTopicResponse rt = new FetchResponseData.FetchableTopicResponse()
                            .setTopic(t.topic()).setTopicId(t.topicId());
                    for (FetchRequestData.FetchPartition p : t.partitions()) {
                        FetchResponseData.PartitionData pd = new FetchResponseData.PartitionData().setPartitionIndex(p.partition())
                                .setHighWatermark(log.size()).setLastStableOffset(log.size()).setLogStartOffset(0)
                                .setRecords(MemoryRecords.EMPTY);
                        if (p.fetchOffset() < log.size()) {
                            int from = (int) p.fetchOffset();
                            pd.setRecords(MemoryRecords.withRecords(from, Compression.NONE,
                                    log.subList(from, log.size()).toArray(new SimpleRecord[0])));
                            any = true;
                        }
                        rt.partitions().add(pd);
                    }
                    d.responses().add(rt);
                }
                long left = deadline - System.currentTimeMillis();
                if (any || left <= 0 || r.topics().isEmpty()) return d;
                wait(left);
            }
        }
    }
}
