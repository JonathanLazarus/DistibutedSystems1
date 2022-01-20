package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.*;
import edu.yu.cs.com3800.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GatewayServer extends Thread implements LoggingServer {
    private final HttpServer server;
    private final GatewayPeerServerImpl observer;
    private final int myPort;
    // gateway assigns IDs to requests
    private final AtomicLong requestIdCount;
    // locally queue requests until a leader is elected
    private final LinkedBlockingQueue<Exchange> queuedRequests;

    private final Logger logger;

    private ThreadLocal<Logger> localLogger;

    public GatewayServer(int port, GatewayPeerServerImpl gatewayPeerServer) throws IOException {
        this.myPort = port;
        this.server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
        this.server.createContext(Util.HTTPENDPOINT, new ClientRequestHandler());
        this.server.setExecutor(Executors.newFixedThreadPool(2)); //Runtime.getRuntime().availableProcessors()));
        this.observer = gatewayPeerServer;
        this.requestIdCount = new AtomicLong(0);
        this.queuedRequests = new LinkedBlockingQueue<>();
        this.logger = initializeLogging(this.getClass().getSimpleName() + "-on-port-" + port);
    }

    public InetSocketAddress getServerAddress() {
        return this.server.getAddress();
    }

    public void shutdown() {
        logger.warning("shutdown called for GatewayPeerServerImpl");
        this.server.stop(0);
        this.observer.shutdown();
        interrupt();
    }

    @Override
    public void run() {
        logger.finest("Starting Gateway thread");
        // start server thread that accepts client requests
        this.server.start();
        // observe leader election results
        Util.startAsDaemon(observer, "Gateway observer thread");
        // Gateway logic:
        // make a thread pool to synchronously send queued client requests to the leader
        ThreadPoolExecutor threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()); //todo *2
        while (!this.isInterrupted()) {
            // wait for a leader if need be
            while (getCurrentLeader() != null) {
                try {
                    // execute client requests, ie send to leader
                    Exchange exch = queuedRequests.take();
                    threadPool.execute(exch);
                    logger.finest("processing " + exch.reqId + ". Leader is " + getCurrentLeader().getPort());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (NullPointerException e) {
                    //in case getCurrentLeader.getPort()
                }
            }
        }
        threadPool.shutdown();
        logger.info("exiting GatewayServer.run()");
    }

    // returns UDP address of current leader
    private InetSocketAddress getCurrentLeader() {
        //return this.observer.getLeaderAddress();
        Vote currLeader = this.observer.getCurrentLeader();
        if (currLeader == null) return null;
        else {
            InetSocketAddress leader = this.observer.getPeerByID(currLeader.getProposedLeaderID());
            logger.finest("current leader: " + leader.getPort());
            return leader;
        }
    }

    // for testing purposes
    protected int getQueuedRequestsSize() {
        return queuedRequests.size();
    }

    // HttpExchange runnable wrapper for later execution
    public class Exchange implements Runnable {
        private final String requestBody;
        private final long requestID;
        private final HttpExchange exchange;

        // for logging
        private final String reqId;

        public Exchange(String requestBody, long requestID, HttpExchange exchange) {
            this.requestBody = requestBody;
            this.requestID = requestID;
            this.exchange = exchange;
            this.reqId = "request [" + requestID + "]";
        }

        @Override
        public void run() {
            if (localLogger == null) localLogger = new ThreadLocal<>();
            if (localLogger.get() == null) {
                try {
                    localLogger.set(initializeLogging("GatewayPool-thread-" + currentThread().getId()));
                } catch (IOException e) {
                    System.out.println("Did not init logger");
                }
            }
            Logger logger = localLogger.get();
            logger.info("Executing client " + reqId);
            // send request to cluster
            try {
                InetSocketAddress leader = getCurrentLeader();
                if (leader == null) {
                    requeueRequest(logger, "leader died, re-queueing " + reqId);
                    return;
                }

                // construct a Message to send to the cluster over TCP
                Message msg = new Message(Message.MessageType.WORK, requestBody.getBytes(), observer.getAddress().getHostName(),
                        observer.getUdpPort(), leader.getHostName(), leader.getPort(), requestID);

                // send the message via TCP client
                TCPClient client = new TCPClient(logger, leader.getHostName(), leader.getPort()+2, ZooKeeperPeerServer.ServerState.LEADING);
                Message response = client.sendMessageToHost(msg);
                if (response.equals(msg)) {
                    // TCPClient had issue connecting to leader
                    // this means that an IO exc was encountered when fulfilling the TCP request via client
                    requeueRequest(logger, "issue connecting to leader");
                }
                // check if the leader failed while servicing the response
                else if (getCurrentLeader() == null || !getCurrentLeader().equals(response.getSenderAddress())) {
                    requeueRequest(logger, "Leader failed while servicing request, yet response was received. Ignoring response");
                }
                // otherwise, accept the response as valid
                else {
                    // send response back to httpClient
                    int responseCode = response.getErrorOccurred() ? 400 : 200;
                    exchange.sendResponseHeaders(responseCode, response.getMessageContents().length);
                    OutputStream out = exchange.getResponseBody();
                    out.write(response.getMessageContents());
                    out.close();
                }
            } catch (IOException e) {
                logger.info(e.getClass().getSimpleName() + " thrown when sending response for request [" + requestID + "] over HTTP back to client");
            }
        }

        private void requeueRequest(Logger logger, String log) {
            try {
                logger.info(log);
                queuedRequests.put(this);
                logger.finest("put " + reqId + " back in queue");
            } catch (InterruptedException e) {
                logger.warning("InterruptedException caught when trying to requeue " + reqId);
            }
        }
    }

    /** This handler handles POST requests to the path “/compileandrun”
     *  and sends request to the Master-Worker cluster for processing
     *  It must make sure that the content type in the client request
     *  is “text/x-java-source” and return a response code of 400 if it is not.
     */
    public class ClientRequestHandler implements HttpHandler, LoggingServer {

        /**
         * Handle the given request and generate an appropriate response.
         * See {@link HttpExchange} for a description of the steps
         * involved in handling an exchange.
         *
         * @param exchange the exchange containing the request from the
         *                 client and used to send the response
         * @throws NullPointerException if exchange is <code>null</code>
         */
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (localLogger == null) localLogger = new ThreadLocal<>();
            if (localLogger.get() == null) localLogger.set(initializeLogging(this.getClass().getSimpleName() + "-on-port-" + myPort + "-thread-" + currentThread().getId()));
            Logger logger = localLogger.get();
            logger.finest("handling exchange");
            // Handles only POST requests

            if (exchange.getRequestMethod().equals("POST")) {
                logger.finest("POST exchange");
                if (exchange.getRequestHeaders().getFirst("Content-Type").equals("text/x-java-source")) {
                    // add to request queue, wrapping the request in a runnable for later execution
                    try {
                        // get a request ID to assign this request
                        long requestID = requestIdCount.getAndIncrement();
                        // read the exchange contents/body
                        InputStream is = exchange.getRequestBody();

                        String body = new String(Util.readAllBytesFromNetwork(is));
                        logger.finest("assigning ID: " + requestID + " to HTTP request with body: " + body);
                        is.close();

                        queuedRequests.put(new Exchange(body, requestID, exchange));
                        logger.finest("passed off request [" + requestID + "] to queue for processing");
                    } catch (InterruptedException e) {
                        logger.log(Level.WARNING, "InterruptedException while trying to queue request in Gateway HttpHandler");
                    }
                } else {
                    logger.finest("exchange Content-Type was not text/x-java-source\nSending 400 response");
                    exchange.sendResponseHeaders(400, 0);
                }
            } else {
                logger.finest("was not a POST request\nSending 400 response");
                byte[] onlyPost = "Only POST is supported\n".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(400, onlyPost.length);
                OutputStream out = exchange.getResponseBody();
                out.write(onlyPost);
                out.close();
            }
        }
    }
}













