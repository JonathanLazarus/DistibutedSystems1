package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;


public class ZooKeeperPeerServerImpl extends Thread implements ZooKeeperPeerServer{
    private final InetSocketAddress myAddress;
    private final int udpPort;
    private final int tcpPort;
    private ServerState state;
    private volatile boolean shutdown;
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final Long id;
    private final long peerEpoch;
    private volatile Vote currentLeader;
    private final Map<Long,InetSocketAddress> peerIDtoAddress;
    private final Map<Long, InetSocketAddress> observerAddresses;

    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;
    private RoundRobinLeader roundRobinLeader;
    private JavaRunnerFollower javaRunnerFollower;

    private Logger logger;

    private final int observers;

    public ZooKeeperPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long,InetSocketAddress> peerIDtoAddress, int observers){
        this.udpPort = myPort;
        this.tcpPort = myPort +2;
        this.observers = observers;
        this.peerEpoch = peerEpoch;
        this.id = id;
        this.myAddress = new InetSocketAddress("localhost", myPort); //@78: initialize with localhost
        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();
        this.state = ServerState.LOOKING; // initialize each server as LOOKING

        this.peerIDtoAddress = peerIDtoAddress;
        this.observerAddresses = new HashMap<>(observers);

        try {
            this.logger = initializeLogging(this.getClass().getSimpleName() + "-on-port-" + myPort);
        } catch (IOException e) {}
    }

    protected Logger getLogger() {
        return this.logger;
    }

    @Override
    public int getTcpPort() {
        return tcpPort;
    }

    @Override
    public void shutdown(){
        logger.info("shutdown() called for server #" + id);
        this.shutdown = true;
        if (this.javaRunnerFollower != null) this.javaRunnerFollower.shutdown();
        if (this.roundRobinLeader != null) this.roundRobinLeader.shutdown();
        this.senderWorker.shutdown();
        this.receiverWorker.shutdown();
    }

    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        logger.info("Leader was set to: " + v.toString() + " in server #" + id);
        this.currentLeader = v;
    }

    @Override
    public Vote getCurrentLeader() {
        return this.currentLeader;
    }

    @Override
    public void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {
        Message msg = new Message(type, messageContents, this.myAddress.getHostString(),
                this.udpPort, target.getHostString(), target.getPort());
        this.outgoingMessages.offer(msg);
    }

    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        for(InetSocketAddress peer : peerIDtoAddress.values()) {
            Message msg = new Message(type, messageContents, this.myAddress.getHostString(),
                    this.udpPort, peer.getHostString(), peer.getPort());
            this.outgoingMessages.offer(msg);
        }
    }

    @Override
    public ServerState getPeerState() {
        return this.state;
    }

    @Override
    public void setPeerState(ServerState newState) {
        this.state = newState;
    }

    @Override
    public Long getServerId() {
        return this.id;
    }

    @Override
    public long getPeerEpoch() {
        return this.peerEpoch;
    }

    @Override
    public InetSocketAddress getAddress() {
        return this.myAddress;
    }

    @Override
    public int getUdpPort() {
        return this.udpPort;
    }

    @Override
    public InetSocketAddress getLeaderAddress() {
        if (currentLeader == null) return null;
        return peerIDtoAddress.get(currentLeader.getProposedLeaderID());
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        return this.peerIDtoAddress.get(peerId);
    }

    /*
    when using this method make sure to use >= and not just >
     */
    @Override
    public int getQuorumSize() {
        //todo Stage 5: fault tolerance
        int clusterSize = peerIDtoAddress.containsKey(this.id) ? peerIDtoAddress.size() : peerIDtoAddress.size()+1;
        // stage 4 logic: subtract observers from the cluster size
        clusterSize -= this.observers;
        // quorum means majority
        // by definition, majority for an even int := (evenInt/2) +1, for an odd int := (oddInt/2) +1
        //(size +1)/2 because the map doesn't contain "this" server, but this should be included to compute quorum
        return (clusterSize/2) +1;
    }

    @Override
    public void run(){
        try {
            //step 1: create and run thread that sends broadcast messages
            this.senderWorker = new UDPMessageSender(this.outgoingMessages, this.udpPort);
            Util.startAsDaemon(senderWorker, "sender thread");

            //step 2: create and run thread that listens for messages sent to this server
            this.receiverWorker = new UDPMessageReceiver(this.incomingMessages, this.myAddress, this.udpPort, this);
            Util.startAsDaemon(receiverWorker, "Receiver thread");

            //step 3: main server loop
            while (!this.shutdown){
                switch (getPeerState()){
                    case LOOKING:
                    case OBSERVER:
                        if (getCurrentLeader() != null) { //todo stage5 implementation
                            break; // so that observers don't keep running election logic forever
                        } // else:
                        logger.fine("Starting leader election as " + getPeerState());
                        //start leader election, set leader to the election winner
                        ZooKeeperLeaderElection election = new ZooKeeperLeaderElection(this, incomingMessages);
                        setCurrentLeader(election.lookForLeader());
                        break;
                    case LEADING:
                        if (this.roundRobinLeader == null) {
                            logger.info("Starting Round Robin Leader algorithm as the leader/master");
                            this.roundRobinLeader = new RoundRobinLeader(this, getFollowerMapping());
                            Util.startAsDaemon(this.roundRobinLeader, "RoundRobinLeader thread");
                        }
                        break;
                    case FOLLOWING:
                        if (this.javaRunnerFollower == null) {
                            logger.info("Starting to follow Master (server #" + getCurrentLeader().getProposedLeaderID() + ")");
                            this.javaRunnerFollower = new JavaRunnerFollower(this);
                            Util.startAsDaemon(this.javaRunnerFollower, "JavaRunnerFollower thread");
                        }
                        break;
                }
            }
        } catch (IOException e) {
            logger.log(Level.WARNING, "Issue initializing & starting UDPMessageReciever/Sender thread", e);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Exception caught in main server loop", e);
        }
    }

    /*
     * returns a map containing id->address mappings of peer servers in FOLLOWING state only.
     * to be passed to RoundRobinLeader so that work is not assigned to OBSERVERS
     */
    private HashMap<Long, InetSocketAddress> getFollowerMapping() {
        // make shallow copy of entire cluster
        HashMap<Long, InetSocketAddress> shallowCopy = new HashMap<>(this.peerIDtoAddress);
        for (Map.Entry<Long, InetSocketAddress> entry : this.peerIDtoAddress.entrySet()) {
            // make a shallow mapping of ID -> TCP addresses
            shallowCopy.put(entry.getKey(), new InetSocketAddress(entry.getValue().getHostName(), entry.getValue().getPort()+2));
        }

        StringBuilder log = new StringBuilder("OBSERVERS are:");
        // remove observers
        for (Long observerID : this.observerAddresses.keySet()) {
            log.append("\n\tServer #").append(observerID);
            shallowCopy.remove(observerID);
        }
        logger.info(log.toString());
        // the observers that we have seen thus far - signified by observerAddresses map constructed based
        // off of messages received in leader election. If the size of ths map is != to the initial amount
        // of observers passed into the constructor as an int, then the wrong mapping of strictly FOLLOWERS
        // will be sent to RRL which will try to assign WORK to an observer. The case in which this occurs
        // is when leader election finishes based on quorum, but the server LOOKING happened to not receive
        // a message from 1+ OBSERVERs in the cluster before getting the quorum. RRL will result in a ConnectException
        if (this.observerAddresses.size() != this.observers) logger.severe("RRL will fail!");
        return shallowCopy;
    }

    public void addObserverToMap(Long observerID, InetSocketAddress observerAddress) {
        this.observerAddresses.put(observerID, observerAddress);
    }

}
