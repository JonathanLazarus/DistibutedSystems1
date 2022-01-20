package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;


public class ZooKeeperPeerServerImpl extends Thread implements ZooKeeperPeerServer{
    private final InetSocketAddress myAddress;
    private final int udpPort;
    private volatile ServerState state;
    private final Long id;
    private long peerEpoch;
    private final int observers;
    private volatile Vote currentLeader, prevLeader;
    private volatile boolean shutdown;

    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final Map<Long,InetSocketAddress> peerIDtoAddress;
    private final ConcurrentHashMap<InetSocketAddress, Long> peerAddressToId;
    private final ConcurrentHashMap<Long, InetSocketAddress> observerAddresses;
    private final ConcurrentHashMap<Long, InetSocketAddress> failedNodes;

    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;

    private Logger logger;

    private final GossipStyleHeartbeat gossip;

    public ZooKeeperPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long,InetSocketAddress> peerIDtoAddress, Long gatewayObserverId, int observers){
        this.udpPort = myPort;
        this.observers = observers;
        this.peerEpoch = peerEpoch;
        this.id = id;
        this.myAddress = new InetSocketAddress("localhost", myPort); //@78: initialize with localhost
        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();
        this.state = ServerState.LOOKING; // initialize each server as LOOKING

        this.peerIDtoAddress = peerIDtoAddress;
        this.observerAddresses = new ConcurrentHashMap<>(observers);
        if (observers > 0 && gatewayObserverId != null && !gatewayObserverId.equals(id)) {
            this.observerAddresses.put(gatewayObserverId, peerIDtoAddress.get(gatewayObserverId));
        }

        this.failedNodes = new ConcurrentHashMap<>();

        try {
            this.logger = initializeLogging(this.getClass().getSimpleName() + "-on-port-" + myPort);
        } catch (IOException e) {}

        this.peerAddressToId = new ConcurrentHashMap<>(peerIDtoAddress.size());
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            this.peerAddressToId.put(entry.getValue(), entry.getKey());
        }

        this.gossip = new GossipStyleHeartbeat(this, incomingMessages, outgoingMessages);

    }

    protected Logger getLogger() {
        return this.logger;
    }

    @Override
    public void shutdown(){
        logger.severe("shutdown() called for server #" + id);
        this.shutdown = true;
        this.senderWorker.shutdown();
        this.receiverWorker.shutdown();
        interrupt();
    }

    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        String vote = v == null ? null : v.toString();
        logger.info("Leader was set to: " + vote + " in server #" + id);
        this.prevLeader = this.currentLeader; // for testing purposes
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
    public void reportFailedPeer(long peerID) {
        InetSocketAddress failedAddress = this.peerIDtoAddress.get(peerID);
        logger.info("failed peer reported: [" + id + "] @ address: " + failedAddress);
        this.failedNodes.put(peerID, failedAddress);
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
    public boolean isPeerDead(long peerID) {
        return failedNodes.containsKey(peerID);
    }

    @Override
    public boolean isPeerDead(InetSocketAddress address) {
        return failedNodes.containsValue(address);
    }

    public Long peerAddressToId(InetSocketAddress address) {
        return peerAddressToId.get(address);
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    public boolean isLeaderDead() {
        if (prevLeader != null && !failedNodes.containsKey(prevLeader.getProposedLeaderID()))
            return true;
        return currentLeader != null &&
                !failedNodes.containsKey(getCurrentLeader().getProposedLeaderID());
    }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        if (peerIDtoAddress.get(peerId) == null)
            logger.warning(peerId +" as long not contained in map");
        return this.peerIDtoAddress.get(peerId);
    }

    /*
    when using this method make sure to use >= and not just >
     */
    @Override
    public int getQuorumSize() {
        int clusterSize = peerIDtoAddress.containsKey(this.id) ? peerIDtoAddress.size() : peerIDtoAddress.size()+1;
        // stage 4 logic: subtract observers from the cluster size
        clusterSize -= this.observers;
        // stage 5 logic:
        clusterSize -= failedNodes.size();
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

            // keep references outside of while loop to LeaderElection, JavaRunnerFollower,
            // and RoundRobinLeader, as they may need to be accessed in later iterations
            ZooKeeperLeaderElection election = new ZooKeeperLeaderElection(this, incomingMessages);
            RoundRobinLeader roundRobinLeader = null;
            JavaRunnerFollower javaRunnerFollower = null;
            //step 3: main server loop
            while (!isInterrupted() && !this.shutdown){
                switch (getPeerState()) {
                    case OBSERVER:
                        // stage5 implementation so that observers don't keep running election logic forever
                        if (isLeaderDead()) {
                            gossip();
                        } //else run an election by falling into next case - LOOKING (no break)
                    case LOOKING:
                        logger.fine("Starting leader election as " + getPeerState());
                        //start leader election, set leader to the election winner
                        setCurrentLeader(election.lookForLeader());
                        break;
                    case LEADING:
                        // if just elected leader
                        if (roundRobinLeader == null) {
                            // collect any completed work from previous followers starting with this server.
                            // RRL will collect other followers completed work

                            // if server was previously a follower, and is now leading, collect its completed work
                            if (javaRunnerFollower != null) {
                                roundRobinLeader = new RoundRobinLeader(this, getFollowerList(),
                                        javaRunnerFollower.getCompletedWork());

                                // now stop javaRunnerFollower thread, as server is now leading
                                javaRunnerFollower.shutdown();
                                javaRunnerFollower = null;
                            }
                            //otherwise, was not a follower beforehand, ie this is the first election, so it can't have completed work as a follower
                            else {
                                // init RRL will null list
                                roundRobinLeader = new RoundRobinLeader(this, getFollowerList(), null);
                            }
                            // start RRL thread
                            logger.info("Starting Round Robin Leader algorithm as the leader/master");
                            Util.startAsDaemon(roundRobinLeader, "RoundRobinLeader thread");
                        }
                        // if RRL is already running, gossip until leader dies
                        gossip();
                        // leader (self) died; stop running logic
                        roundRobinLeader.shutdown();
                        break;
                    case FOLLOWING:
                        if (javaRunnerFollower == null) {
                            logger.info("Starting to follow Master (server #" + getCurrentLeader().getProposedLeaderID() + ")");
                            javaRunnerFollower = new JavaRunnerFollower(this);
                            Util.startAsDaemon(javaRunnerFollower, "JavaRunnerFollower thread");
                        }
                        // if server is already running JRF, gossip until leader dies
                        else gossip();
                        break;
                }
            }
            // exited server logic while loop
            logger.severe("[" + this.id + "] Exiting ZookeeperPeerServerImpl.run()");
            this.senderWorker.interrupt();
            this.receiverWorker.interrupt();
            if (javaRunnerFollower != null) {
                javaRunnerFollower.shutdown();
                javaRunnerFollower = null;
            }
            if (roundRobinLeader != null) {
                roundRobinLeader.shutdown();
                roundRobinLeader = null;
            }
        } catch (IOException e) {
            logger.log(Level.WARNING, "Issue initializing & starting UDPMessageReceiver/Sender thread", e);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Exception caught in main server loop", e);
        }

    }

    private void gossip() {
        gossip.beginProtocol();
        switch(getPeerState()) {
            case FOLLOWING:
                Util.logAndPrint(logger, Level.INFO, "[" + id + "]: leader failed, switching from " + getPeerState() + " to " + ServerState.LOOKING.name());
                break;
            case LEADING:
                return;
            default:
                break;
        }
        // reset the leader
        try {
            setCurrentLeader(null);
        } catch (IOException e) {
            this.currentLeader = null;
        }
        // set new state (OBSERVER doesn't change)
        this.setPeerState(ServerState.LOOKING);
        // increase epoch
        peerEpoch++;
    }


    /*
     * returns a list containing ids of peer servers in FOLLOWING state only.
     * to be passed to RoundRobinLeader so that work is not assigned to OBSERVERS
     */
    private List<Long> getFollowerList() {
        // make shallow copy of entire cluster ids
        ArrayList<Long> shallowCopy = new ArrayList<>(this.peerIDtoAddress.size());
        for (Long id : this.peerIDtoAddress.keySet()) {
            // unbox so we lose the reference
            long unboxed = id;
            // make a shallow copy of list of IDs
            shallowCopy.add(unboxed);
        }
        // remove observers
        shallowCopy.removeAll(observerAddresses.keySet());
        // remove failed modes
        shallowCopy.removeAll(failedNodes.keySet());
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

    public ArrayList<Long> getAllIds() {
        ArrayList<Long> L = new ArrayList<>(peerIDtoAddress.size());
        L.addAll(peerIDtoAddress.keySet());
        return L;
    }
}
