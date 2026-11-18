import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * KAFKA-17263 against a real broker: one round. No fault injection: the fence comes from a second consumer started
 * with the same group.instance.id, and the rebalance from a new member joining.
 *
 *   A   static member (group.instance.id "a"); its application catches FencedInstanceIdException and keeps polling
 *   A2  a second consumer with group.instance.id "a" (a duplicate, as in a botched deploy); joins once and stops
 *   S   a dynamic member; stops polling at step 4, so the rebalance stays in progress until it is done
 *   N   a dynamic member that joins at step 4
 *
 * 1. A and S join and are stable.
 * 2. A2 joins: the broker replaces A's member id with A2's, and A's next heartbeat gets FENCED_INSTANCE_ID. A's poll()
 *    throws; A's application catches it and polls on, so A rejoins (fencing A2 in turn). A2 is closed.
 * 3. A runs normally for 2 s.
 * 4. S stops polling and N joins: a rebalance. A's onPartitionsRevoked marks the start of its rejoin; its JoinGroup
 *    waits at the broker for S.
 * 5. A's application polls 500 ms more, then calls close(Duration.ofSeconds(5)).
 *
 * Checked: does A's close() return (30 s watchdog), and how many heartbeat threads are left once A, A2, S and N
 * are closed (a hung A keeps its own). Prints which heartbeat threads exist before the close; the "current" one is read reflectively
 * (AbstractCoordinator.heartbeatThread), for the report only.
 *
 *   java DuplicateInstanceClose BOOTSTRAP GROUP
 */
public class DuplicateInstanceClose {
    static final String HB = "kafka-coordinator-heartbeat-thread";
    static final String TOPIC = "t";

    public static void main(String[] args) throws Exception {
        String bootstrap = args[0];
        String group = args[1];

        Member s = new Member(props(bootstrap, group, null, 60_000), "S");
        Member a = new Member(props(bootstrap, group, "a", 300_000), "A");
        s.start();
        a.start();
        await(() -> a.assigned() && s.assigned(), 30, "A and S joined");

        Member a2 = new Member(props(bootstrap, group, "a", 300_000), "A2");
        a2.stopAfterJoin = true;
        a2.start();
        await(() -> a.fences.get() >= 1, 30, "A fenced (FencedInstanceIdException caught by A's application)");
        await(() -> a.assigned() && a.joinsCompleted.get() >= 2, 30, "A rejoined after the fence");
        a2.closeAndWait(10);
        Thread.sleep(2000);

        s.stall.set(true);
        Member n = new Member(props(bootstrap, group, null, 300_000), "N");
        n.start();
        if (!a.revoked.await(30, TimeUnit.SECONDS)) throw new IllegalStateException("no rebalance seen by A");
        Thread.sleep(500);

        Thread current = currentHeartbeatThread(a.consumer);
        Thread ofS = currentHeartbeatThread(s.consumer);
        Thread ofN = currentHeartbeatThread(n.consumer);
        System.out.println("A: fenced " + a.fences.get() + " time(s), rebalance in progress (A's JoinGroup waiting for S)");
        System.out.println("heartbeat threads before A's close():");
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().startsWith(HB) && t.isAlive()) {
                String who = t == current ? "A's current heartbeat thread" : t == ofS ? "S's" : t == ofN ? "N's"
                        : "no consumer's: dropped by A's pollHeartbeat after the fence, not closed";
                System.out.println("    #" + t.getId() + " " + t.getState() + "  " + who);
            }
        }
        long t0 = System.currentTimeMillis();
        a.close.set(true);
        boolean returned = a.closed.await(30, TimeUnit.SECONDS);
        long closeMs = System.currentTimeMillis() - t0;
        System.out.println("A: close(PT5S) " + (returned ? "returned in " + closeMs + " ms" : "HUNG (still running after 30 s)"));
        if (!returned) {
            for (StackTraceElement e : a.thread.getStackTrace()) {
                if (e.getClassName().startsWith("org.apache.kafka") || e.getMethodName().equals("join")) System.out.println("    at " + e);
            }
        }

        n.closeAndWait(10);
        s.stall.set(false);
        s.closeAndWait(10);
        Thread.sleep(500);
        long left = Thread.getAllStackTraces().keySet().stream().filter(t -> t.getName().startsWith(HB) && t.isAlive()).count();
        System.out.println("heartbeat threads alive after closing A, A2, S, N: " + left);
        System.out.println("ROUND " + (returned ? "returned" : "hung") + " " + (returned ? closeMs : -1) + " leaked " + left);
        System.exit(0);
    }

    static Properties props(String bootstrap, String group, String instanceId, int maxPollIntervalMs) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        p.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic");
        if (instanceId != null) p.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId);
        p.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.toString(maxPollIntervalMs));
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return p;
    }

    interface Cond {
        boolean ok();
    }

    static void await(Cond c, int seconds, String what) throws InterruptedException {
        long end = System.currentTimeMillis() + seconds * 1000L;
        while (!c.ok()) {
            if (System.currentTimeMillis() > end) throw new IllegalStateException("timed out waiting for: " + what);
            Thread.sleep(20);
        }
    }

    /** One consumer and its application thread: poll in a loop, catch FencedInstanceIdException and carry on. */
    static final class Member implements ConsumerRebalanceListener {
        final Properties props;
        final String name;
        final Thread thread;
        volatile KafkaConsumer<String, String> consumer;
        volatile Collection<TopicPartition> assignment = List.of();
        final AtomicInteger fences = new AtomicInteger();
        final AtomicInteger joinsCompleted = new AtomicInteger();
        final AtomicBoolean stall = new AtomicBoolean();
        final AtomicBoolean close = new AtomicBoolean();
        final CountDownLatch closed = new CountDownLatch(1);
        final CountDownLatch revoked = new CountDownLatch(1);
        volatile boolean stopAfterJoin;
        volatile boolean watchRevoke;

        Member(Properties props, String name) {
            this.props = props;
            this.name = name;
            this.thread = new Thread(this::run, "app-" + name);
            this.thread.setDaemon(true);
        }

        void start() {
            thread.start();
        }

        boolean assigned() {
            return !assignment.isEmpty();
        }

        void run() {
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(List.of(TOPIC), this);
            while (!close.get()) {
                if (stall.get() || (stopAfterJoin && joinsCompleted.get() > 0)) {
                    sleep(20);
                    continue;
                }
                try {
                    consumer.poll(Duration.ofMillis(100));
                } catch (FencedInstanceIdException e) {
                    fences.incrementAndGet(); // the application logs it and keeps going
                }
            }
            consumer.close(Duration.ofSeconds(5));
            closed.countDown();
        }

        void closeAndWait(int seconds) throws InterruptedException {
            close.set(true);
            if (!closed.await(seconds, TimeUnit.SECONDS)) System.out.println(name + ": close() did not return in " + seconds + " s");
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            if (joinsCompleted.get() >= 2 && name.equals("A")) revoked.countDown();
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            assignment = List.copyOf(consumer.assignment());
            joinsCompleted.incrementAndGet();
        }

        static void sleep(long ms) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

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
                    if (name.equals("delegate")) continue;
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
}
