package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.ZooKeeperPeerServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState.FOLLOWING;

/*
 * run on the leader node.It assigns work it receives to followers on a round-robin basis,
 * gets the results back from the followers, and sends the responses to the one who
 * requested the work, i.e. the client.
 */
public class RoundRobinLeader implements LoggingServer {
    private Logger logger;
    private final ZooKeeperPeerServer myPeerServer;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final LinkedList<Map.Entry<Long, InetSocketAddress>> workers;

    public RoundRobinLeader(ZooKeeperPeerServer server, LinkedBlockingQueue<Message> incomingMessages, LinkedBlockingQueue<Message> outgoingMessages, Map<Long, InetSocketAddress> peerIdtoAddress) {
        try {
            this.logger = initializeLogging(RoundRobinLeader.class.getCanonicalName() + "-on-port-" + server.getAddress().getPort());
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.myPeerServer = server;
        this.incomingMessages = incomingMessages;
        this.outgoingMessages = outgoingMessages;
        this.workers = new LinkedList<>(peerIdtoAddress.entrySet());
        logger.config("Round-Robin algorithm being led by Server #" + myPeerServer.getServerId());
    }

    public synchronized void lead() {
        // maps request IDs to client address so we know where to send the response once the worker completed the request
        HashMap<Long, InetSocketAddress> requestIdToClient = new HashMap<>();
        long requestIdCount = 0; // current ID to use for new requests
        int timeOut = Util.finalizeWait;
        Message msg = null;
        // first get work from client/completed work from workers
        while (myPeerServer.getPeerState() == ZooKeeperPeerServer.ServerState.LEADING && !myPeerServer.isShutdown()) {
            try {
                //Remove next notification from queue
                msg = incomingMessages.poll(timeOut, TimeUnit.MILLISECONDS);
                //if no notifications received
                if (msg == null) {
                    // and implement exponential back-off when notifications not received
                    timeOut = Math.min(timeOut*2, Util.maxNotificationInterval);
                }
                // differentiate between completed work received from workers and work sent by client
                else if (msg.getMessageType() == Message.MessageType.WORK) {
                    // received work from client

                    // assign work ID to each request and keep track of requests to send responses
                    requestIdToClient.put(requestIdCount, msg.getSenderAddress());
                    // assign work to workers on round-robin basis
                    sendMessageToNextWorker(msg.getMessageContents(), requestIdCount);
                    requestIdCount++;
                } else if (msg.getMessageType() == Message.MessageType.COMPLETED_WORK) {
                    // completed work from a worker

                    // send response to the client that requested the work
                    sendMessageToClient(msg, requestIdToClient.remove(msg.getRequestID()));
                }
            } catch (InterruptedException e) {
                logger.log(Level.WARNING, "InterruptedException caught when trying to receive message", e);
            }


        }
        if (myPeerServer.isShutdown()) logger.severe("Peer server was shutdown");
        else logger.severe("Peer server not in LEADING state anymore");
    }

    private void sendMessage(Message.MessageType type, byte[] content, InetSocketAddress targetAddress, long requestId, boolean errorOccurred) {
        Message m = new Message(type, content, myPeerServer.getAddress().getHostName(), myPeerServer.getUdpPort(),
                targetAddress.getHostName(), targetAddress.getPort(), requestId, errorOccurred);
        outgoingMessages.offer(m);
    }

    private void sendMessageToNextWorker(byte[] content, long requestId) {
        // get next worker (Round Robin)
        Map.Entry<Long, InetSocketAddress> entry = workers.remove();
        if (entry != null) {
            logger.fine("Assigning work with request id #" + requestId + " to next worker (ID#" + entry.getKey() + ", PORT#"
                    + entry.getValue().getPort() + ") on round-robin basis");
            sendMessage(Message.MessageType.WORK, content, entry.getValue(), requestId, false);
            // put worker back at end of queue
            workers.add(entry);
        }
    }

    private void sendMessageToClient(Message msg, InetSocketAddress targetAddress) {
        sendMessage(Message.MessageType.COMPLETED_WORK, msg.getMessageContents(), targetAddress, msg.getRequestID(), msg.getErrorOccurred());
    }
}
