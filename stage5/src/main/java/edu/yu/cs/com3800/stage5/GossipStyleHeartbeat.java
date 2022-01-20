package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.ZooKeeperPeerServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GossipStyleHeartbeat implements LoggingServer {
    private final ZooKeeperPeerServerImpl myPeer;
    private Logger logger;
    private final AtomicLong heartbeatCounter;
    private final ConcurrentHashMap<Long, Long> idToHeartbeat, idToLocalTime, failedNodes;

    private final ArrayList<Gossip> gossipHistory;
    private final ArrayList<Long> allIds;

    // UDP queues
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final LinkedBlockingQueue<Message> incomingMessages;

    public GossipStyleHeartbeat(ZooKeeperPeerServerImpl myPeer, LinkedBlockingQueue<Message> incomingMessages,
                                LinkedBlockingQueue<Message> outgoingMessages) {
        this.myPeer = myPeer;
        this.incomingMessages = incomingMessages;
        this.outgoingMessages = outgoingMessages;
        this.gossipHistory = new ArrayList<>();

        this.heartbeatCounter = new AtomicLong(0);
        // init heartbeat table
        this.idToHeartbeat = new ConcurrentHashMap<>();
        this.idToLocalTime = new ConcurrentHashMap<>();

        this.failedNodes = new ConcurrentHashMap<>();

        this.allIds = this.myPeer.getAllIds();

        try {
            this.logger = initializeLogging(this.getClass().getSimpleName() + " on-server-" + myPeer.getServerId(),
                    "/logs/gossip");
        } catch (IOException e) {
            System.out.println("IOE caught when initializing logger for gossip of server " + myPeer.getServerId());
        }
    }

    public void beginProtocol() {
        // create a timer instance to send out gossip every GOSSIP milliseconds
        Timer heartbeatTimer = new Timer();
        // make a schedule for the timer to send out gossip messages periodically
        heartbeatTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                // gossip my data:
                // pick a random node to gossip to that is not marked failed already
                long id = -1;
                while (id == -1 || failedNodes.containsKey(id)) {
                    int randomIndex = ThreadLocalRandom.current().nextInt(allIds.size());
                    id = allIds.get(randomIndex);
                }
                // get its address
                InetSocketAddress target = myPeer.getPeerByID(id);
                // increment my heartbeat counter
                idToHeartbeat.put(myPeer.getServerId(), heartbeatCounter.incrementAndGet());
                // make the gossip message
                Message msg = new Message(Message.MessageType.GOSSIP, getGossipTable(), "localhost",
                        myPeer.getUdpPort(), target.getHostName(), target.getPort());
                outgoingMessages.offer(msg);
            }
        }, 0, Util.GOSSIP);

        // loop as long as leader hasn't failed, and we are not in the LOOKING state
        while (myPeer.isLeaderDead() && !myPeer.isShutdown() &&
                !myPeer.getPeerState().equals(ZooKeeperPeerServer.ServerState.LOOKING)) {
            if (myPeer.isShutdown()) {

                System.out.println(myPeer.getServerId() + ": returning from gossip protocol, server shutdown");
                return;
            }
            try {
                // retrieve next message
                Message msg = null;
                int timeOut = Util.finalizeWait;
                while (msg == null) {
                    //Remove next notification from queue, timing out after 2 times the termination time
                    msg = incomingMessages.poll(timeOut, TimeUnit.MILLISECONDS);
                    // implement exponential back-off when notifications not received
                    timeOut = Math.min(timeOut * 2, Util.maxNotificationInterval);
                }
                // we've got a message. Check that the server that sent it is still alive
                if (myPeer.isPeerDead(msg.getSenderAddress())) continue;
                // If it is not a GOSSIP message, put it back and continue until it is
                else if (!msg.getMessageType().equals(Message.MessageType.GOSSIP)) {
                    incomingMessages.offer(msg);
                    continue;
                }
                // otherwise, we have a GOSSIP message
                Gossip gsp = new Gossip(msg.getMessageContents());
                gossipHistory.add(gsp); // add to gossip history
                // update my table based on received gossip
                boolean leaderFailed = updateMembership(gsp, heartbeatTimer);
                if (leaderFailed) {
                    logger.fine(myPeer.getServerId() + ": leader failed");
                    // stop gossiping and return to run election logic
                    heartbeatTimer.cancel();
                    return;
                }
            } catch (InterruptedException e) {
            }
        }
        heartbeatTimer.cancel();
        logger.info("Exiting gossip protocol");
    }

    // returns true if leader failed
    private boolean updateMembership(Gossip gsp, Timer timer) {
        for (Long id : gsp.getTable().keySet()) {
            // if the server in the gossip table hasn't already been marked as dead
            if (!id.equals(myPeer.getServerId()) && !failedNodes.containsKey(id)) {

                // if this is the first time we are seeing this id:
                if (!idToHeartbeat.containsKey(id)) {
                    // add id to our table
                    idToHeartbeat.put(id, gsp.getHeartbeat(id));
                    idToLocalTime.put(id, gsp.getTimeReceived());
                    logger.fine(String.format("[%d]: updated [%d]'s heartbeat sequence to %d based on message from [%d] at node time %d",
                            myPeer.getServerId(), id, gsp.getHeartbeat(id), gsp.getSenderId(), gsp.getTimeReceived()));
                }
                // if we already have this id in the table:
                else {
                    long prevHeartbeat = idToHeartbeat.get(id);
                    long prevTime = idToLocalTime.get(id);
                    // check if the server's heartbeat timed out
                    if (System.currentTimeMillis() > prevTime + Util.FAIL) {
                        Util.logAndPrint(logger, Level.INFO,
                                String.format("[%d]: no heartbeat from server [%d] - server failed",
                                myPeer.getServerId(), id));
                        // mark server as failed
                        failedNodes.put(id, prevTime);
                        myPeer.reportFailedPeer(id);
                        // if the leader failed, return true to stop gossip logic so we enter new election
                        if (id == myPeer.getCurrentLeader().getProposedLeaderID()) return true;
                        // else, schedule a cleanup
                        timer.schedule(new TimerTask() {
                            @Override
                            public void run() {
                                idToHeartbeat.remove(id);
                            }
                        }, Util.CLEANUP);
                    }
                    // heartbeat didn't time out
                    else {
                        long heartbeat = gsp.getHeartbeat(id);
                        if (heartbeat <= prevHeartbeat) continue;
                        // update our table
                        idToHeartbeat.put(id, gsp.getHeartbeat(id));
                        idToLocalTime.put(id, gsp.getTimeReceived());
                        String log = String.format("[%d]: updated [%d]'s heartbeat sequence to %d based on message from [%d] at node time %d",
                                myPeer.getServerId(), id, gsp.getHeartbeat(id), gsp.getSenderId(), gsp.getTimeReceived());
                        logger.finer(log);
                    }

                }
            }
        }
        // leader was not marked dead
        return false;
    }

    // serializes table
    public byte[] getGossipTable() {
        /*
        n = table.size()
        table entry size = 2 * long = 2* 8 bytes = 16 bytes

        size of buffer =
        1 long (senderId) = 8 bytes
        n entries = n * 2 * long = n * 16 bytes
         = 8 + 16n
         */
        int bufferSize = 8 + (16 * idToHeartbeat.size());
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        buffer.clear();
        buffer.putLong(this.myPeer.getServerId());
        for (Map.Entry<Long, Long> tableEntry : idToHeartbeat.entrySet()) {
            buffer.putLong(tableEntry.getKey());
            buffer.putLong(tableEntry.getValue());
        }
        buffer.flip();
        return buffer.array();
    }



}
