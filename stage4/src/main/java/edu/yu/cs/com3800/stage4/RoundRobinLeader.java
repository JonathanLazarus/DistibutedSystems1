package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * run on the leader node.It assigns work it receives to followers on a round-robin basis,
 * gets the results back from the followers, and sends the responses to the one who
 * requested the work, i.e. the client.
 */
public class RoundRobinLeader extends Thread implements LoggingServer {
    private Logger logger;
    private final ZooKeeperPeerServerImpl myPeerServer;
    private final LinkedBlockingQueue<MasterTcpServer.TcpWrapper> tcpRequests;
    private MasterTcpServer tcpServer;
    private ConcurrentHashMap<Long, InetSocketAddress> requestIdToClient = new ConcurrentHashMap<>(); // maps request IDs to client address so we know where to send the response once the worker completed the request

    // thread-safe linked list to represent the round-robin basis
    private LinkedBlockingDeque<Map.Entry<Long, InetSocketAddress>> roundRobin;

    // logger used by threads in thread pool
    private ThreadLocal<Logger> localLogger;

    public RoundRobinLeader(ZooKeeperPeerServerImpl server, Map<Long, InetSocketAddress> peerIdtoAddress) {
        this.myPeerServer = server;
        this.tcpRequests = new LinkedBlockingQueue<>();

        try {
            this.logger = initializeLogging(this.getClass().getSimpleName() + "-on-Tcp-port-" + server.getTcpPort());
            this.tcpServer = new MasterTcpServer(this.myPeerServer.getTcpPort(), this.tcpRequests);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // initialize the thread-safe round-robin list
        this.roundRobin = new LinkedBlockingDeque<>(peerIdtoAddress.size());
        this.roundRobin.addAll(peerIdtoAddress.entrySet());

        logger.config("Round-Robin algorithm being led by Server #" + myPeerServer.getServerId());
        //todo remove
        Util.logMap(logger, Level.FINE, peerIdtoAddress);
    }

    @Override
    public void run() {
        Util.startAsDaemon(this.tcpServer, "master tcp server");
        long requestIdCount = 0; // current ID to use for new requests

        int timeOut = Util.finalizeWait;
        ThreadPoolExecutor threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()); //todo *2
        MasterTcpServer.TcpWrapper msg = null;
        // first get work from client/completed work from workers
        while (!this.isInterrupted()) {
            try {
                //Remove next notification from queue
                msg = tcpRequests.poll(timeOut, TimeUnit.MILLISECONDS);
                //if no notifications received
                if (msg == null) {
                    // and implement exponential back-off when notifications not received
                    timeOut = Math.min(timeOut*2, Util.maxNotificationInterval);
                }
                else {
                    logger.info("new Socket request received");

                    // assign work to workers on round-robin basis:
                    // get next worker (Round Robin)
                    Map.Entry<Long, InetSocketAddress> entry = roundRobin.remove();

                    logger.info("Assigning received socket request to next worker (ID#"
                            + entry.getKey() + ", TCP PORT#" + entry.getValue().getPort() + ") on round-robin basis");

                    // create "Work" for the ThreadPool to execute synchronously (over TCP)
                    Work w = new Work(msg, requestIdCount++, entry);
                    // put worker back at end of queue (for Round-Robin)
                    roundRobin.add(entry);

                    // synchronously process the client request
                    threadPool.execute(w);


                }
            } catch (InterruptedException e) {
                logger.log(Level.WARNING, "InterruptedException caught when trying to receive message", e);
            }


        }
    }

    public void shutdown() {
        logger.info("shutdown() called for RoundRobinLeader thread on peer #" + myPeerServer.getServerId());
        interrupt();
    }

    public class Work implements Runnable {

        private Message msg;
        private OutputStream out;
        private final long requestId;
        private Map.Entry<Long, InetSocketAddress> worker;


        public Work(MasterTcpServer.TcpWrapper request, long requestId, Map.Entry<Long, InetSocketAddress> worker) {
            this.msg = request.getMsg();
            this.out = request.getOut();
            this.requestId = requestId;
            this.worker = worker;
        }

        @Override
        public void run() {
            // initialize logger local to this thread
            if (localLogger == null) localLogger = new ThreadLocal<>();
            if (localLogger.get() == null) {
                try {
                    localLogger.set(initializeLogging("RRL-" + myPeerServer.getTcpPort() + "-on-thread-" + currentThread().getId()));
                } catch (IOException e) {
                    logger.log(Level.WARNING, "IOE thrown in RRL thread pool thread trying to init logger", e);
                }
            }
            Logger logger = localLogger.get();

            try {
                // assign work ID to each request and keep track of requests to send responses
                requestIdToClient.put(this.requestId, msg.getSenderAddress());

                // reconstruct message
                msg = new Message(Message.MessageType.WORK, msg.getMessageContents(),
                        myPeerServer.getAddress().getHostName(), myPeerServer.getTcpPort(),
                        worker.getValue().getHostName(), worker.getValue().getPort(), this.requestId, false);

                logger.fine("Assigning work with request id #" + requestId + " to next worker (ID#"
                        + worker.getKey() + ", TCP PORT#" + msg.getReceiverPort() + ") on round-robin basis");

                // open tcp connection with target worker via client
                TCPClient client = new TCPClient(logger, worker.getValue().getHostName(), worker.getValue().getPort());
                // send message and wait for response
                msg = client.sendMessageToHost(msg);

                // send message back to tcp server
                logger.finest("Sending response back to cluster's gateway TCP server to send back to original client");
                out.write(msg.getNetworkPayload());
            } catch (ConnectException e) {
                // special case - means that the port we got was not one of the FOLLOWER ports
                // so we need to remove it from the round-robin list
                logger.info("Removing non-FOLLOWER (ID#" + worker.getKey() + ", TCP PORT#"
                        + worker.getValue().getPort() + ") from round-robin list");
                if (roundRobin.remove(this.worker)) {
                    logger.info("worker removed. reassigning request");
                    this.worker = roundRobin.remove(); // get next worker
                    roundRobin.add(this.worker); //add back to RR list
                    this.run(); // retry
                }
            } catch (IOException e) {
                logger.log(Level.WARNING, "IOE caught when dealing with Socket streams");
            }
        }
    }
}
