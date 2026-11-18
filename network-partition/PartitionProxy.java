import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * A TCP proxy in front of one Kafka broker listener, with a network partition switch.
 *
 * While partitioned, nothing is forwarded in either direction: bytes wait in the kernel's socket buffers, and new
 * connections are accepted but carry nothing, as with packets dropped by a partition. On heal, forwarding resumes
 * where it stopped (as TCP retransmission would). All proxies share one switch: {@link #partition} cuts the client
 * off from every broker.
 *
 * The proxy reads Kafka's frame headers as they pass (size, API key, correlation id), like a packet capture, so a
 * nemesis can act on a protocol event: {@link #onJoinGroupRequest} fires when a JoinGroup request (API key 11) goes
 * out, and {@link #onJoinGroupResponseDelivered} after its response has been written to the client's socket. Nothing
 * inside the client is touched.
 */
public final class PartitionProxy implements AutoCloseable {

    private static final Object SWITCH = new Object();
    private static boolean partitioned;

    public static volatile Runnable onJoinGroupRequest = () -> { };
    /** Runs on the proxy thread right after the response is written, before any further byte is forwarded. */
    public static volatile Runnable onJoinGroupResponseDelivered = () -> { };

    public static void partition() {
        synchronized (SWITCH) { partitioned = true; }
    }

    public static void heal() {
        synchronized (SWITCH) { partitioned = false; SWITCH.notifyAll(); }
    }

    static void awaitConnected() throws InterruptedException {
        synchronized (SWITCH) { while (partitioned) SWITCH.wait(); }
    }

    private final ServerSocket server;
    private final int target;
    private final List<Socket> sockets = new CopyOnWriteArrayList<>();

    public PartitionProxy(int listenPort, int targetPort) throws IOException {
        this.target = targetPort;
        this.server = new ServerSocket();
        server.setReuseAddress(true);
        server.bind(new InetSocketAddress("127.0.0.1", listenPort));
        Thread t = new Thread(this::acceptLoop, "proxy-" + listenPort);
        t.setDaemon(true);
        t.start();
    }

    private void acceptLoop() {
        while (!server.isClosed()) {
            try {
                Socket client = server.accept();
                Socket broker = new Socket("127.0.0.1", target);
                client.setTcpNoDelay(true);
                broker.setTcpNoDelay(true);
                sockets.add(client);
                sockets.add(broker);
                Connection c = new Connection();
                start("up", () -> pump(client, broker, true, c));
                start("down", () -> pump(broker, client, false, c));
            } catch (IOException e) {
                // closed
            }
        }
    }

    private static void start(String name, Runnable r) {
        Thread t = new Thread(r, "proxy-" + name);
        t.setDaemon(true);
        t.start();
    }

    /** Per-connection protocol state: the correlation id of the JoinGroup request in flight, if any. */
    private static final class Connection {
        volatile int joinCorrelationId = -1;
    }

    /**
     * Copies one direction. Requests: [int32 size][int16 apiKey][int16 apiVersion][int32 correlationId]...
     * Responses: [int32 size][int32 correlationId]... Only these header bytes are looked at.
     */
    private static void pump(Socket from, Socket to, boolean request, Connection c) {
        byte[] buf = new byte[16384];
        byte[] head = new byte[12];
        int headLen = request ? 12 : 8;
        int headFill = 0;
        long frameLeft = 0;   // payload bytes of the current frame still to pass (after its 4-byte size)
        boolean deliverHook = false;
        try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
            for (;;) {
                int n = in.read(buf);
                if (n < 0) break;
                awaitConnected();   // after the read: bytes that arrive during a partition wait for the heal
                int off = 0;
                while (off < n) {
                    // Forward up to the end of the current frame, so a hook can run between frames.
                    int chunk;
                    if (frameLeft == 0 && headFill < headLen) {
                        // collecting a header; forward byte by byte through the header
                        head[headFill++] = buf[off];
                        chunk = 1;
                        if (headFill == headLen) {
                            int size = ((head[0] & 0xff) << 24) | ((head[1] & 0xff) << 16) | ((head[2] & 0xff) << 8) | (head[3] & 0xff);
                            frameLeft = size - (headLen - 4);
                            if (request) {
                                int apiKey = ((head[4] & 0xff) << 8) | (head[5] & 0xff);
                                int corr = ((head[8] & 0xff) << 24) | ((head[9] & 0xff) << 16) | ((head[10] & 0xff) << 8) | (head[11] & 0xff);
                                if (apiKey == 11) {
                                    c.joinCorrelationId = corr;
                                    onJoinGroupRequest.run();
                                }
                            } else {
                                int corr = ((head[4] & 0xff) << 24) | ((head[5] & 0xff) << 16) | ((head[6] & 0xff) << 8) | (head[7] & 0xff);
                                if (corr == c.joinCorrelationId) {
                                    c.joinCorrelationId = -1;
                                    deliverHook = true;
                                }
                            }
                        }
                    } else {
                        chunk = (int) Math.min(n - off, frameLeft);
                        frameLeft -= chunk;
                    }
                    out.write(buf, off, chunk);
                    off += chunk;
                    if (frameLeft == 0 && headFill == headLen) {
                        headFill = 0;
                        if (deliverHook) {
                            deliverHook = false;
                            out.flush();
                            onJoinGroupResponseDelivered.run();
                        }
                        // the rest of this read may belong to the next frame: respect a partition set meanwhile
                        if (off < n) awaitConnected();
                    }
                }
                out.flush();
            }
        } catch (IOException | InterruptedException e) {
            // connection closed
        } finally {
            try { from.close(); } catch (IOException ignored) { }
            try { to.close(); } catch (IOException ignored) { }
        }
    }

    @Override
    public void close() throws IOException {
        server.close();
        for (Socket s : sockets) {
            try { s.close(); } catch (IOException ignored) { }
        }
    }
}
