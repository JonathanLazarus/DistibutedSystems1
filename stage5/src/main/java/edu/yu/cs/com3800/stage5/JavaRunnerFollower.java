package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * run on a worker node. When the leader assigns this node some work to do,
 * this class uses a JavaRunner to do the work, and returns the results back to the leader.
 */
public class JavaRunnerFollower extends Thread implements LoggingServer {
    private Logger logger;
    private final ZooKeeperPeerServerImpl myPeerServer;
    private final LinkedBlockingQueue<Message> completedWork; // queue of completed work

    public JavaRunnerFollower(ZooKeeperPeerServerImpl server) {
        this.myPeerServer = server;
        this.completedWork = new LinkedBlockingQueue<>();
        try {
            this.logger = initializeLogging(this.getClass().getSimpleName() + "-on-port-" + server.getUdpPort());
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.config("Server #" + myPeerServer.getServerId() + " starting as JavaRunnerWorker");
    }

    @Override
    public void run() {
        logger.finest("JavaRunnerFollower.run() invoked");

        // loop, accepting all requests from leader
        while (!this.isInterrupted() && !myPeerServer.isShutdown() &&
                myPeerServer.getPeerState() == ZooKeeperPeerServer.ServerState.FOLLOWING) {
            try (ServerSocket serverSocket = new ServerSocket(myPeerServer.getUdpPort() + 2)) {
                serverSocket.setSoTimeout(4000);
                // wait for a request
                Socket clientSocket = serverSocket.accept();
                logger.finest("got client connection from " + clientSocket.getPort());
                InputStream in = clientSocket.getInputStream();
                Message request = new Message(Util.readAllBytesFromTcp(in, clientSocket));
                logger.finer("Received request [" + request.getRequestID() + "] from server ["
                        + myPeerServer.peerAddressToId(request.getSenderAddress()) + "]");

                Message response;
                // switch on type of request
                switch (request.getMessageType()) {
                    case WORK:
                        // javaRunner logic:
                        try {
                            JavaRunner javaRunner = new JavaRunner();
                            logger.finest("Compiling java program with request ID #" + request.getRequestID());
                            ByteArrayInputStream in1 = new ByteArrayInputStream(request.getMessageContents());
                            String output = javaRunner.compileAndRun(in1);
                            logger.fine("Program output for request #" + request.getRequestID() + " was: " + output);
                            response = new Message(Message.MessageType.COMPLETED_WORK, output.getBytes(StandardCharsets.UTF_8),
                                    request.getReceiverHost(), request.getReceiverPort(), request.getSenderHost(), request.getSenderPort(),
                                    request.getRequestID(), false);
                        } catch (Exception e) {
                            StringBuilder sb = new StringBuilder();
                            sb.append(e.getMessage());
                            sb.append("\n");
                            sb.append(Util.getStackTrace(e));
                            logger.log(Level.WARNING, sb.toString());
                            response = new Message(Message.MessageType.COMPLETED_WORK, sb.toString().getBytes(StandardCharsets.UTF_8),
                                    request.getReceiverHost(), request.getReceiverPort(), request.getSenderHost(), request.getSenderPort(),
                                    request.getRequestID(), true);
                        }
                        break;
                    case NEW_LEADER_GETTING_LAST_WORK:
                        // leader sends NEW_LEADER_GETTING_LAST_WORK messages to each follower until the
                        // follower responds with a message with invalid requestID (< 0)
                        response = hasCompletedWork() ? completedWork.take() :
                                // send response back to leader with empty contents and invalid requestID
                                // to signify there is no queued up completed work on this node
                                new Message(Message.MessageType.COMPLETED_WORK, "".getBytes(),
                                        request.getReceiverHost(), request.getReceiverPort(),
                                        request.getReceiverHost(), request.getSenderPort(), -1);
                        break;
                    default:
                        // ignore requests of all other types
                        continue; // get next request from leader
                }

                // back to TCP server logic. Send response back to leader/sender:

                // check if leader/sender is dead before sending back
                if (myPeerServer.isPeerDead(request.getSenderAddress())) {
                    // peer is dead - save work in queue
                    logger.info("locally saving completed work with ID [" + request.getRequestID() + "] until new leader requests it");
                    this.completedWork.put(response);
                } else {
                    // leader/sender not dead - send back the response
                    OutputStream out = clientSocket.getOutputStream();
                    out.write(response.getNetworkPayload());
                    logger.finest("sending request [" + request.getRequestID() + "] response to leader");
                    // client socket has been closed
                    clientSocket.close();
                }
            } catch (SocketTimeoutException e) {
                // stops blocking of accept so we don't get a bind exception if this peer becomes a leader
                // .accept() timed out, see if we are still following, ie continue in loop
                logger.finer("socket timed out");
                continue;
            } catch (IOException e) {
                logger.log(Level.WARNING, "issue with server socket", e);
            } catch (InterruptedException e) {
                logger.log(Level.WARNING, "issue saving completed work");
            }
        }
        logger.severe("[" +myPeerServer.getServerId() + "] Exiting JavaRunnerFollower.run()");
    }

    public void shutdown() {
        logger.info("shutdown() called for JavaRunnerFollower thread on peer #" + myPeerServer.getServerId());
        this.interrupt();
    }

    protected boolean hasCompletedWork() {
        return !this.completedWork.isEmpty();
    }

    protected LinkedBlockingQueue<Message> getCompletedWork() {
        return this.completedWork;
    }
}
