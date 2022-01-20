package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * run on a worker node. When the leader assigns this node some work to do,
 * this class uses a JavaRunner to do the work, and returns the results back to the leader.
 */
public class JavaRunnerFollower extends Thread implements LoggingServer {
    private TCPServer tcpReceiver;
    private Logger logger;
    private final ZooKeeperPeerServer myPeerServer;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final LinkedBlockingQueue<Message> outgoingMessages;

    public JavaRunnerFollower(ZooKeeperPeerServer server) {
        this.myPeerServer = server;
        this.incomingMessages = new LinkedBlockingQueue<>();
        this.outgoingMessages = new LinkedBlockingQueue<>();
        try {
            this.logger = initializeLogging(this.getClass().getSimpleName() + "-on-port-" + server.getUdpPort());
            this.tcpReceiver = new TCPServer(server.getTcpPort(), incomingMessages, outgoingMessages);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Util.startAsDaemon(this.tcpReceiver, "TCP receiver on master");
        logger.config("Server #" + myPeerServer.getServerId() + " starting as JavaRunnerWorker");
    }

    @Override
    public void run() {
        int timeOut = Util.finalizeWait;
        //Loop, receiving work from master
        while (!this.isInterrupted()) {
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
                    logger.finest("Message with request ID #" + msg.getRequestID() + " has program:\n\t" + program);
                    ByteArrayInputStream in = new ByteArrayInputStream(program.getBytes());
                    String output = javaRunner.compileAndRun(in);
                    logger.fine("Program output for request #" + msg.getRequestID() + " was: " + output);
                    sendResponseToLeader(msg, false, output);
                }
            } catch (InterruptedException e) {
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
    }

    public void shutdown() {
        logger.info("shutdown() called for JavaRunnerFollower thread on peer #" + myPeerServer.getServerId());
        tcpReceiver.interrupt();
        interrupt();
    }

    // sends response back over TCP via queue
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
