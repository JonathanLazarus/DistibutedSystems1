package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState.FOLLOWING;

/*
 * run on a worker node. When the leader assigns this node some work to do,
 * this class uses a JavaRunner to do the work, and returns the results back to the leader.
 */
public class JavaRunnerFollower extends Thread implements LoggingServer {
    private Logger logger;
    private final ZooKeeperPeerServer myPeerServer;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final LinkedBlockingQueue<Message> outgoingMessages;

    public JavaRunnerFollower(ZooKeeperPeerServer server, LinkedBlockingQueue<Message> incomingMessages, LinkedBlockingQueue<Message> outgoingMessages) {
        try {
            this.logger = initializeLogging(JavaRunnerFollower.class.getCanonicalName() + "-on-port-" + server.getUdpPort());
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.myPeerServer = server;
        this.incomingMessages = incomingMessages;
        this.outgoingMessages = outgoingMessages;
        logger.config("Server #" + myPeerServer.getServerId() + " starting as JavaRunnerWorker");
    }

    public synchronized void work() {
        int timeOut = Util.finalizeWait;
        //Loop, receiving work from master
        while (myPeerServer.getPeerState() == FOLLOWING && !myPeerServer.isShutdown()) {
            Message msg = null;
            try {
                //Remove next notification from queue, timing out after 2 times the termination time
                msg = incomingMessages.poll(timeOut, TimeUnit.MILLISECONDS);
                //if no notifications received
                if (msg == null) {
                    // implement exponential back-off when notifications not received
                    timeOut = Math.min(timeOut * 2, Util.maxNotificationInterval);
                } else if (msg.getMessageType() == Message.MessageType.WORK) {
                    JavaRunner javaRunner = new JavaRunner();
                    String program = new String(msg.getMessageContents());
                    logger.finer("Message with request ID #" + msg.getRequestID() + " has program:\n\t" + program);
                    ByteArrayInputStream in = new ByteArrayInputStream(program.getBytes());
                    String output = javaRunner.compileAndRun(in);
                    logger.fine("Program output for request #" + msg.getRequestID() + " was: " + output);
                    sendResponseToLeader(msg, false, output);
                }
            }
            //todo implement logging and responses in catch clauses; clean up catch clauses
            catch (InterruptedException e) {
                logger.log(Level.WARNING, "InterruptedException caught when trying to receive message", e);
            } catch (IllegalArgumentException e) {
                logger.log(Level.WARNING, e.getMessage(), e);
            } catch (Exception e) {
                StringBuilder sb = new StringBuilder();
                sb.append(e.getMessage());
                sb.append("\n");
                sb.append(Util.getStackTrace(e));
                logger.log(Level.WARNING, sb.toString());
                sendResponseToLeader(msg, true, sb.toString());
            }
        }
        if (myPeerServer.isShutdown()) logger.severe("Peer server was shutdown");
        else logger.severe("Peer server not in FOLLOWING state anymore");
    }

    private void sendResponseToLeader(Message msg, boolean errorOccurred, String response) {
        if (msg != null) {
            Message m = new Message(Message.MessageType.COMPLETED_WORK, response.getBytes(),
                    myPeerServer.getAddress().getHostName(), myPeerServer.getAddress().getPort(),
                    myPeerServer.getLeaderAddress().getHostName(), myPeerServer.getLeaderAddress().getPort(),
                    msg.getRequestID(), errorOccurred);
            outgoingMessages.offer(m);
        }
    }

}
