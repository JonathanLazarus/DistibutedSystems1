package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.ZooKeeperPeerServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
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

    // thread-safe linked list to represent the round-robin basis
    private final LinkedBlockingQueue<Long> roundRobin;
    private final ConcurrentHashMap<Long, Message> completedWork; // requestID -> completed work

    // logger used by threads in thread pool
    private ThreadLocal<Logger> localLogger;

    public RoundRobinLeader(ZooKeeperPeerServerImpl server, List<Long> peerIds, LinkedBlockingQueue<Message> completedWork) {
        this.myPeerServer = server;
        this.tcpRequests = new LinkedBlockingQueue<>();
        // initialize the thread-safe round-robin list
        this.roundRobin = new LinkedBlockingQueue<>(peerIds);
        this.completedWork = new ConcurrentHashMap<>();
        // put all previously completed work server may or may not have completed as a follower
        if (completedWork != null && !completedWork.isEmpty()){
            for (Message m : completedWork) {
                if (m != null) this.completedWork.put(m.getRequestID(), m);
            }
        }

        try {
            this.logger = initializeLogging(this.getClass().getSimpleName() + "-on-Tcp-port-" + server.getUdpPort());
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.config("Round-Robin algorithm being led by Server #" + myPeerServer.getServerId());
        //todo remove
        Util.logList(logger, Level.FINE, peerIds);
    }

    @Override
    public void run() {
        // create thread pool to service requests synchronously
        ThreadPoolExecutor threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        // first, collect completed work from other workers
        if (myPeerServer.getPeerEpoch() > 0) {
            // note: workers would only have completed work if this leader is at least teh second leader, ie epoch > 0
            collectCompletedWork(threadPool);
        }
        MasterTcpServer tcpServer = new MasterTcpServer(this.myPeerServer, this.tcpRequests);
        // start getting requests/work from client using TCPServer thread
        Util.startAsDaemon(tcpServer, "master tcp server");
        // completed work from workers
        while (!this.isInterrupted() && !myPeerServer.isShutdown()) {
            try {
                //Remove next notification from queue
                MasterTcpServer.TcpWrapper msg = tcpRequests.take(); // msg is not null using take()
                if (!myPeerServer.isPeerDead(msg.getMsg().getSenderAddress())){
                    logger.info("new Socket request received");
                    // assign work to workers on round-robin basis:
                    // get next worker alive (Round Robin)
                    long nextID = roundRobin.take();
                    while (myPeerServer.isPeerDead(nextID)) {
                        // don't continue to assign work if peer is marked dead,
                        // just get next worker, without re-adding failed worker
                        logger.warning("RRL skipping dead peer [" + nextID + "]");
                        nextID = roundRobin.take();
                    }

                    InetSocketAddress udpAddress = myPeerServer.getPeerByID(nextID);
                    logger.info("Assigning received socket request with requestID #" + msg.getMsg().getRequestID()
                            + " to next worker (ID#" + nextID + ", TCP PORT#" + (udpAddress.getPort()+2)
                            + ") on round-robin basis");

                    // create "Work" for the ThreadPool to execute synchronously (over TCP)
                    Work w = new Work(msg, nextID, udpAddress);
                    // put worker back at end of queue (for Round-Robin)
                    roundRobin.put(nextID);

                    // synchronously process the client request
                    threadPool.execute(w);


                } else {
                    logger.finest(msg.getMsg().getSenderAddress().toString());
                    logger.finest("sender peer [" + myPeerServer.peerAddressToId(msg.getMsg().getSenderAddress()) + "] is marked dead. Skipping message.");
                }
            } catch (InterruptedException e) {
                logger.log(Level.WARNING, "InterruptedException caught when trying to receive message", e);
            }


        }
        tcpServer.interrupt();
        logger.severe("Exiting RRL.run()");
    }

    private void collectCompletedWork(ThreadPoolExecutor threadPool) {
        logger.fine("RRL beginning to collect work from other followers");
        // create a second round-robin list to modify for the purposes of this method
        // IDs in this list still have completed work / need to be queried for completed work
        LinkedList<Long> rr = new LinkedList<>(this.roundRobin);
        while (!this.isInterrupted() && !rr.isEmpty()) {
            // get next worker alive (Round Robin)
            Long nextID = rr.remove();
            while (myPeerServer.isPeerDead(nextID)) {
                // don't continue to query if peer is marked dead,
                // just get next worker to query, without re-adding failed worker
                logger.warning("RRL skipping dead peer [" + nextID + "]");
                nextID = roundRobin.remove();
            }

            InetSocketAddress udpAddress = myPeerServer.getPeerByID(nextID);
            logger.info("Querying next worker (ID#" + nextID + ", TCP PORT#" + (udpAddress.getPort()+2) + ") for COMPLETED_WORK");
            // create "Query" for the ThreadPool to execute synchronously (over TCP)
            Query q = new Query(nextID, udpAddress);
            // once we query a worker/follower, we don't ask for completed work again

            // synchronously process the client request
            threadPool.execute(q);
        }
    }

    public void shutdown() {
        logger.info("shutdown() called for RoundRobinLeader thread on peer #" + myPeerServer.getServerId());
        interrupt();
    }

    public class Work implements Runnable {
        private final Message msg;
        private final OutputStream out;
        private Long workerId;
        private InetSocketAddress targetUdpAddress;

        public Work(MasterTcpServer.TcpWrapper request, Long workerId, InetSocketAddress udpAddress) {
            this.msg = request.getMsg();
            this.out = request.getOut();
            this.workerId = workerId;
            this.targetUdpAddress = udpAddress;
        }

        @Override
        public void run() {
            // initialize logger local to this thread
            if (localLogger == null) localLogger = new ThreadLocal<>();
            if (localLogger.get() == null) {
                try {
                    localLogger.set(initializeLogging("RRL-" + myPeerServer.getUdpPort() + "-on-thread-" + currentThread().getId()));
                } catch (IOException e) {
                    logger.log(Level.WARNING, "IOE thrown in RRL thread pool thread trying to init logger", e);
                }
            }
            Logger logger = localLogger.get();

            Message response;
            if (completedWork.containsKey(msg.getRequestID())) {
                logger.finer("Already completed work with request ID [" + msg.getRequestID() + "]");
                response = completedWork.remove(msg.getRequestID());

            } else {
                // reconstruct message
                Message request = new Message(Message.MessageType.WORK, msg.getMessageContents(),
                        myPeerServer.getAddress().getHostName(), myPeerServer.getUdpPort(),
                        targetUdpAddress.getHostName(), targetUdpAddress.getPort(), msg.getRequestID(), false);

                logger.fine("Assigning work with request id #" + msg.getRequestID() + " to next worker (ID#"
                        + workerId + ", TCP PORT#" + (targetUdpAddress.getPort() + 2) + ") on round-robin basis");

                // open tcp connection with target worker via client
                TCPClient client = new TCPClient(logger, targetUdpAddress.getHostName(), targetUdpAddress.getPort() + 2,
                        ZooKeeperPeerServer.ServerState.FOLLOWING);
                // send message and wait for response
                response = client.sendMessageToHost(request);
                // if the response equals our original request, that means that an error occurred during the TCP connection
                if (request.equals(response)) {
                    logger.warning("TCP connection to worker [" + workerId + " failed while servicing request [" + msg.getRequestID() + "]. Retrying");
                    // check if the worker failed
                    if (myPeerServer.isPeerDead(workerId)) {
                        // remove dead worker from round-robin as not to assign it anymore work
                        if (roundRobin.remove(workerId)) {
                            logger.info("Removed dead worker [" + workerId + "] from round robin. Will no longer assign work to it");
                        }
                    }
                    // reassign work
                    logger.warning("reassigning request [" + msg.getRequestID() + "]");
                    this.workerId = roundRobin.remove(); // get next worker
                    this.targetUdpAddress = myPeerServer.getPeerByID(workerId);
                    roundRobin.add(workerId); //add back to RR list
                    this.run(); // retry
                    return;
                }
            }
            // otherwise, we have a valid response:
            // reconstruct message to send back to gateway
            response = new Message(response.getMessageType(), response.getMessageContents(),
                    myPeerServer.getAddress().getHostName(), myPeerServer.getUdpPort(),
                    msg.getSenderHost(), msg.getSenderPort(),
                    response.getRequestID(), response.getErrorOccurred());
            try {
                // send message back to tcp server
                logger.finest("Sending response [" + response.getRequestID() + "] back to cluster's gateway TCP server to send back to original client");
                out.write(response.getNetworkPayload());
            } catch (IOException e) {
                logger.warning(e.getClass().getSimpleName() + " caught when writing response [" + msg.getRequestID() + "] to gateway server via TCP OutputStream");
            }
        }
    }

    public class Query implements Runnable {
        private Long id; // target server's ID
        private InetSocketAddress udpAddress; // udp address of target

        public Query(Long targetId, InetSocketAddress targetUdpAddress) {
            if (targetId == null || targetUdpAddress == null)
                throw new IllegalArgumentException("initialized Query will null params");
            this.id = targetId;
            this.udpAddress = targetUdpAddress;
        }

        @Override
        public void run() {
            // initialize logger local to this thread
            if (localLogger == null) localLogger = new ThreadLocal<>();
            if (localLogger.get() == null) {
                try {
                    localLogger.set(initializeLogging("RRL$Query-" + myPeerServer.getUdpPort() + "-on-thread-" + currentThread().getId()));
                } catch (IOException e) {
                    logger.log(Level.WARNING, "IOE thrown in RRL thread pool thread trying to init logger", e);
                }
            }
            Logger logger = localLogger.get();

            Message request = Message.getCollectCompletedWorkMsg(myPeerServer.getAddress(), udpAddress);
            // open tcp connection with target worker via client
            TCPClient client = new TCPClient(logger, udpAddress.getHostName(), udpAddress.getPort()+2,
                    ZooKeeperPeerServer.ServerState.FOLLOWING);
            // leader sends NEW_LEADER_GETTING_LAST_WORK messages to each follower until the
            // follower responds with a message with invalid request ID (< 0)

            // loop - keep sending queries until above conditions are met, or peer dies
            Message response = client.sendMessageToHost(request);
            while(!myPeerServer.isPeerDead(id) && response.getRequestID() > -1 &&
                    response.getMessageType() == Message.MessageType.COMPLETED_WORK) {
                // we got a valid response of completed work - put it in completedWork
                completedWork.put(response.getRequestID(), response);
                logger.fine("new leader received completed work with request id [" + response.getRequestID() + "]");
                // keep querying until follower has no more completed work
                response = client.sendMessageToHost(request);
            }
            // if the response equals our original request, that means that an error occurred during the TCP connection
            if (request.equals(response)) {
                logger.warning("TCP connection failed while servicing querying server [" + id + "]");
            }
        }
    }
}
