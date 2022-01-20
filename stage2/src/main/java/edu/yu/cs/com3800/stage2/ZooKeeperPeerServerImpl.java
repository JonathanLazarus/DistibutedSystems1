package edu.yu.cs.com3800.stage2;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;


public class ZooKeeperPeerServerImpl implements ZooKeeperPeerServer{
    private final InetSocketAddress myAddress;
    private final int myPort;
    private ServerState state;
    private volatile boolean shutdown;
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final Long id;
    private final long peerEpoch;
    private volatile Vote currentLeader;
    private final Map<Long,InetSocketAddress> peerIDtoAddress;

    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;

    private Logger logger;

    public ZooKeeperPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long,InetSocketAddress> peerIDtoAddress){
        this.myPort = myPort;
        this.peerEpoch = peerEpoch;
        this.id = id;
        this.myAddress = new InetSocketAddress("localhost", myPort); //@78: initialize with localhost
        this.peerIDtoAddress = peerIDtoAddress;
        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();
        this.state = ServerState.LOOKING; // initialize each server as LOOKING
        try {
            this.logger = initializeLogging(ZooKeeperPeerServerImpl.class.getCanonicalName() + "-on-port-" + myPort, true);
        } catch (IOException e) {
            //e.printStackTrace();
            logger.log(Level.WARNING, "issue initializing logger", e);
        }
    }

    @Override
    public void shutdown(){
        logger.fine("shutdown() called for server #" + id);
        this.shutdown = true;
        this.senderWorker.shutdown();
        this.receiverWorker.shutdown();
    }

    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        logger.info("Leader was set to: " + v.toString() + " in server #" + id);
        this.currentLeader = v;
        this.shutdown();
    }

    @Override
    public Vote getCurrentLeader() {
        return this.currentLeader;
    }

    @Override
    public void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {
        Message msg = new Message(type, messageContents, this.myAddress.getHostString(),
                this.myPort, target.getHostString(), target.getPort());
        this.outgoingMessages.offer(msg);
    }

    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        for(InetSocketAddress peer : peerIDtoAddress.values()) {
            Message msg = new Message(type, messageContents, this.myAddress.getHostString(),
                    this.myPort, peer.getHostString(), peer.getPort());
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
        return this.myPort;
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
        // quorum means majority
        // by definition, majority for an even int := (evenInt/2) +1, for an odd int := (oddInt/2) +1
        //(size +1)/2 because the map doesn't contain "this" server, but this should be included to compute quorum
        return (clusterSize/2) +1;
    }

    @Override
    public void run(){
        try {
            //step 1: create and run thread that sends broadcast messages
            this.senderWorker = new UDPMessageSender(this.outgoingMessages, this.myPort);
            this.senderWorker.start();
            //step 2: create and run thread that listens for messages sent to this server
            this.receiverWorker = new UDPMessageReceiver(this.incomingMessages, this.myAddress, this.myPort, this);
            this.receiverWorker.start();
        } catch (IOException e) {
            logger.log(Level.WARNING, "issue initializing & starting UDPMessageReciever/Sender thread", e);
        }
        //todo rest
        //step 3: main server loop
        try{
            while (!this.shutdown){
                switch (getPeerState()){
                    case LOOKING:
                        logger.fine("starting leader election");
                        //start leader election, set leader to the election winner
                        ZooKeeperLeaderElection election = new ZooKeeperLeaderElection(this, this.incomingMessages);
                        setCurrentLeader(election.lookForLeader());
                        break;
                    //todo: not stage 2
                    case LEADING:
                    case FOLLOWING:
                    case OBSERVER:
                        break;
                }
            }
            this.shutdown();
        }
        catch (Exception e) {
           logger.log(Level.WARNING, "exception caught in main server loop", e);
        }
    }

}
