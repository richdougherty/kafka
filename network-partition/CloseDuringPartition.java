import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * KAFKA-17734 on a real three-broker cluster, with the fault at the network level only.
 *
 * Each round uses a new group. Consumer L (the group leader: it joins first) reaches the brokers through
 * {@link PartitionProxy}s; S, M, the producer and the admin client connect directly.
 *
 *   1. L joins, then S joins; both consume topic "events" while a producer feeds it.
 *   2. S's application goes busy (stops polling; its heartbeat thread keeps it in the group).
 *   3. M joins, subscribed to "events" and to a topic L has never fetched metadata for. The group rebalances.
 *      L's next poll sends its JoinGroup, which the coordinator holds until S rejoins; the poll returns.
 *   4. L's application goes busy (it is processing records), then S polls again and rejoins. The coordinator
 *      answers every JoinGroup. Once L's response has been written to L's socket, the nemesis partitions L from
 *      all brokers (the proxies stop forwarding).
 *   5. L's application shuts down: close(Duration.ZERO). A watchdog waits 30 s; if close hasn't returned it dumps
 *      L's stack, heals the partition and times how long close then takes.
 *
 * Nothing in the client is changed or hooked. Which kafka-clients classes run (published, or with a fix compiled
 * ahead of the jar) is up to the class path.
 *
 *   java -cp ... CloseDuringPartition <eager|cooperative|default> <rounds> <variant-name>
 */
public class CloseDuringPartition {

    static final String DIRECT = "localhost:19101,localhost:19102,localhost:19103";
    static final String PROXIED = "localhost:19401,localhost:19402,localhost:19403";
    static final long WATCHDOG_MS = 30_000;
    static final long AFTER_HEAL_MS = 120_000;

    /** A consumer's application thread: polls in a loop until told to go busy; can then rejoin or close. */
    static final class App implements Runnable {
        final String name;
        final KafkaConsumer<String, String> consumer;
        final List<String> topics;
        volatile boolean busy;
        volatile boolean stop;
        volatile boolean closeZero;
        final CountDownLatch busyAck;
        volatile CountDownLatch resume = new CountDownLatch(0);
        final AtomicLong consumed = new AtomicLong();
        volatile long closeStart, closeEnd;
        final Thread thread;

        App(String name, Properties p, List<String> topics) {
            this.name = name;
            this.consumer = new KafkaConsumer<>(p);
            this.topics = topics;
            this.busyAck = new CountDownLatch(1);
            this.thread = new Thread(this, "app-" + name);
            thread.setDaemon(true);
        }

        @Override
        public void run() {
            consumer.subscribe(topics);
            try {
                while (!stop) {
                    if (busy) {
                        busyAck.countDown();
                        while (busy && !closeZero && !stop) {
                            try { resume.await(50, TimeUnit.MILLISECONDS); } catch (InterruptedException e) { return; }
                        }
                        if (closeZero) break;
                        continue;
                    }
                    ConsumerRecords<String, String> rs = consumer.poll(Duration.ofMillis(100));
                    consumed.addAndGet(rs.count());
                }
            } catch (Exception e) {
                System.out.println("  " + name + " poll: " + e);
            }
            closeStart = System.nanoTime();
            try {
                consumer.close(closeZero ? Duration.ZERO : Duration.ofSeconds(5));
            } catch (Exception e) {
                System.out.println("  " + name + " close: " + e);
            }
            closeEnd = System.nanoTime();
        }

        boolean hasAssignment() {
            return consumed.get() > 0;
        }
    }

    static Properties consumerProps(String bootstrap, String group, String id, String assignor) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, id);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        p.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");
        if (assignor != null) p.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, assignor);
        return p;
    }

    static boolean await(java.util.function.BooleanSupplier c, long ms) throws InterruptedException {
        long end = System.currentTimeMillis() + ms;
        while (!c.getAsBoolean()) {
            if (System.currentTimeMillis() > end) return false;
            Thread.sleep(20);
        }
        return true;
    }

    public static void main(String[] args) throws Exception {
        // eager: RangeAssignor; cooperative: CooperativeStickyAssignor; default: the client's default list
        String assignor = switch (args[0]) {
            case "eager" -> RangeAssignor.class.getName();
            case "cooperative" -> CooperativeStickyAssignor.class.getName();
            case "default" -> null;
            default -> throw new IllegalArgumentException("mode: eager, cooperative or default");
        };
        int rounds = Integer.parseInt(args[1]);
        String variant = args[2];

        List<PartitionProxy> proxies = new ArrayList<>();
        for (int i = 1; i <= 3; i++) proxies.add(new PartitionProxy(19400 + i, 19300 + i));

        Properties ap = new Properties();
        ap.put("bootstrap.servers", DIRECT);
        Properties pp = new Properties();
        pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DIRECT);
        pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pp.put(ProducerConfig.LINGER_MS_CONFIG, "5");

        int hangs = 0, clean = 0, skipped = 0;
        String tag = variant + " " + args[0];
        try (Admin admin = Admin.create(ap); KafkaProducer<String, String> producer = new KafkaProducer<>(pp)) {
            if (!admin.listTopics().names().get().contains("events")) {
                admin.createTopics(List.of(new NewTopic("events", 6, (short) 3))).all().get();
            }
            Thread feeder = new Thread(() -> {
                long n = 0;
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        producer.send(new ProducerRecord<>("events", Long.toString(n % 64), "v" + n++));
                        Thread.sleep(2);
                    } catch (InterruptedException | IllegalStateException e) {
                        return;
                    }
                }
            }, "producer");
            feeder.setDaemon(true);
            feeder.start();

            for (int r = 1; r <= rounds; r++) {
                String run = variant.replaceAll("\\W", "") + "-" + args[0] + "-" + System.currentTimeMillis();
                String group = "g-" + run;
                String extra = "extra-" + run;
                admin.createTopics(List.of(new NewTopic(extra, 1, (short) 3))).all().get();

                final boolean[] sawJoin = new boolean[1];
                PartitionProxy.onJoinGroupRequest = () -> { };
                PartitionProxy.onJoinGroupResponseDelivered = () -> { };

                App l = new App("L", consumerProps(PROXIED, group, "L", assignor), List.of("events"));
                l.thread.start();
                if (!await(l::hasAssignment, 30_000)) { System.out.println(tag + " round " + r + ": L never consumed; skipped"); skipped++; l.stop = true; continue; }
                App s = new App("S", consumerProps(DIRECT, group, "S", assignor), List.of("events"));
                s.thread.start();
                if (!await(s::hasAssignment, 30_000)) { System.out.println(tag + " round " + r + ": S never consumed; skipped"); skipped++; l.stop = s.stop = true; continue; }

                // 2. S busy
                s.resume = new CountDownLatch(1);
                s.busy = true;
                s.busyAck.await(10, TimeUnit.SECONDS);

                // 3. arm: L's next JoinGroup request makes L's application go busy after its poll returns
                CountDownLatch lJoinSent = new CountDownLatch(1);
                PartitionProxy.onJoinGroupRequest = lJoinSent::countDown;
                App m = new App("M", consumerProps(DIRECT, group, "M", assignor), List.of("events", extra));
                m.thread.start();
                if (!lJoinSent.await(30, TimeUnit.SECONDS)) {
                    System.out.println(tag + " round " + r + ": L never rejoined; skipped");
                    skipped++;
                    l.stop = s.stop = m.stop = true; s.busy = false;
                    continue;
                }
                PartitionProxy.onJoinGroupRequest = () -> { };
                l.busy = true;
                l.busyAck.await(10, TimeUnit.SECONDS);

                // 4. the nemesis: partition L once its JoinGroup response is in L's socket
                CountDownLatch delivered = new CountDownLatch(1);
                PartitionProxy.onJoinGroupResponseDelivered = () -> { PartitionProxy.partition(); delivered.countDown(); };
                s.busy = false;
                s.resume.countDown();
                if (!delivered.await(30, TimeUnit.SECONDS)) {
                    System.out.println(tag + " round " + r + ": L's JoinGroup response never came; skipped");
                    skipped++;
                    PartitionProxy.onJoinGroupResponseDelivered = () -> { };
                    l.stop = s.stop = m.stop = true;
                    continue;
                }
                PartitionProxy.onJoinGroupResponseDelivered = () -> { };
                Thread.sleep(200);

                // 5. L shuts down
                l.closeZero = true;
                l.resume.countDown();
                boolean returned = await(() -> l.closeEnd != 0, WATCHDOG_MS);
                if (returned) {
                    clean++;
                    System.out.printf("%s round %d: close(Duration.ZERO) returned in %d ms during the partition%n",
                            tag, r, TimeUnit.NANOSECONDS.toMillis(l.closeEnd - l.closeStart));
                    PartitionProxy.heal();
                } else {
                    hangs++;
                    System.out.printf("%s round %d: close(Duration.ZERO) HUNG (still blocked after %d s of partition)%n",
                            tag, r, WATCHDOG_MS / 1000);
                    if (hangs == 1 || r == rounds) {
                        for (StackTraceElement e : l.thread.getStackTrace()) {
                            String s1 = e.toString();
                            if (s1.contains("org.apache.kafka") || s1.contains("CloseDuringPartition")) System.out.println("      at " + s1);
                        }
                    }
                    long healAt = System.nanoTime();
                    PartitionProxy.heal();
                    boolean after = await(() -> l.closeEnd != 0, AFTER_HEAL_MS);
                    System.out.println(after
                            ? String.format("      after the partition healed, close returned %d ms later (%d s in all)",
                                    TimeUnit.NANOSECONDS.toMillis(l.closeEnd - healAt),
                                    TimeUnit.NANOSECONDS.toSeconds(l.closeEnd - l.closeStart))
                            : "      still blocked " + AFTER_HEAL_MS / 1000 + " s after the partition healed");
                }
                s.stop = m.stop = true;
                s.thread.join(15_000);
                m.thread.join(15_000);
                admin.deleteTopics(Set.of(extra));
            }
            feeder.interrupt();
            feeder.join(5_000);
        } finally {
            PartitionProxy.heal();
            for (PartitionProxy p : proxies) p.close();
        }
        System.out.printf("SUMMARY %s: %d rounds, close(Duration.ZERO) hung %d, returned %d, skipped %d%n",
                tag, rounds, hangs, clean, skipped);
        System.exit(0);
    }
}
