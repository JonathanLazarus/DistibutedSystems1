package edu.yu.cs.com3800;

import edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState.*;

public class ZooKeeperLeaderElection implements LoggingServer {
    private final ZooKeeperPeerServer myPeerServer;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private long proposedLeader, proposedEpoch;
    private final Map<Long, ElectionNotification> voterIDtoVote;
    private Logger logger;

    public ZooKeeperLeaderElection(ZooKeeperPeerServer server, LinkedBlockingQueue<Message> incomingMessages) {
        this.myPeerServer = server;
        this.incomingMessages = incomingMessages;
        this.voterIDtoVote = new HashMap<>();
        try {
            this.logger = initializeLogging(this.getClass().getSimpleName() + "-on-port-" + myPeerServer.getUdpPort());
        } catch (IOException e) {
            e.printStackTrace();
        }
        //servers initial vote is for themselves
        this.setProposal(this.myPeerServer.getServerId(), this.myPeerServer.getPeerEpoch());
    }

    private void setProposal(long proposedLeaderID, long proposedEpoch) {
        logger.fine("changing current proposal from " + getCurrentVote() + " to " + new Vote(proposedLeaderID, proposedEpoch));
        // change vote
        this.proposedLeader = proposedLeaderID;
        this.proposedEpoch = proposedEpoch;
        // count my current vote towards the quorum
        this.voterIDtoVote.put(myPeerServer.getServerId(), new ElectionNotification(myPeerServer.getPeerState(), getCurrentVote(), myPeerServer.getServerId()));
    }

    public static ElectionNotification getNotificationFromMessage(Message received) {
        if (received == null) return null;
        else if (received.getMessageType() == Message.MessageType.ELECTION) {
            ByteBuffer msgBytes = ByteBuffer.wrap(received.getMessageContents());
            long leader = msgBytes.getLong();
            char stateChar = msgBytes.getChar();
            long senderID = msgBytes.getLong();
            long peerEpoch = msgBytes.getLong();
            return new ElectionNotification(leader,
                            ZooKeeperPeerServer.ServerState.getServerState(stateChar),
                            senderID, peerEpoch);
        }
        else return null;
    }

    private synchronized Vote getCurrentVote() {
        return new Vote(this.proposedLeader, this.proposedEpoch);
    }

    public synchronized Vote lookForLeader() {
        if (myPeerServer.isShutdown()) {
            logger.fine("shutdown called on my node. Exiting election");
            return null;
        }
        voterIDtoVote.clear();
        this.proposedLeader = myPeerServer.getServerId();
        this.proposedEpoch = myPeerServer.getPeerEpoch();
        //send initial notifications to other peers to get things started
        sendNotifications();
        int timeOut = Util.finalizeWait;
        ElectionNotification vote;
        //Loop, exchanging notifications with other servers until we find a leader
        while (myPeerServer.getPeerState() == LOOKING || myPeerServer.getPeerState() == OBSERVER) {
            try {
                //Remove next notification from queue, timing out after 2 times the termination time
                Message msg = incomingMessages.poll(timeOut, TimeUnit.MILLISECONDS);
                //if no notifications received
                if (msg == null) {
                    //resend notifications to prompt a reply from others
                    sendNotifications();
                    // and implement exponential back-off when notifications not received
                    timeOut = Math.min(timeOut * 2, Util.maxNotificationInterval);
                    continue;
                }
                else if (!msg.getMessageType().equals(Message.MessageType.ELECTION)){
                    // message was not of type election, therefore ignore it
                    continue;
                }
                if (!this.myPeerServer.isPeerDead(msg.getSenderAddress())) { // check if this server is a valid server
                    vote = getNotificationFromMessage(msg);
                    // ignore election notifications of lesser epochs
                    if (vote == null || vote.getPeerEpoch() < this.proposedEpoch) continue;
                    // otherwise, we have a valid vote - switch on the state of the sender:
                    logger.info("Received vote: " + vote + " from server #" + vote.getSenderID());
                    switch (vote.getState()) {
                        case LOOKING: //if the sender is also looking
                            //if the received message has a vote for a leader which supersedes mine,
                            if (supersedesCurrentVote(vote.getProposedLeaderID(), vote.getPeerEpoch())) {
                                // change vote
                                setProposal(vote.getProposedLeaderID(), vote.getPeerEpoch());
                                // tell all my peers what my new vote is
                                sendNotifications();
                            }
                            //keep track of the votes I received and who I received them from.
                            voterIDtoVote.put(vote.getSenderID(), vote); // each server only gets one vote
                            //// check if I have enough votes to declare my currently proposed leader as the leader, but only once you receive a vote that you think would count towards the quorum
                            if (vote.getProposedLeaderID() == this.proposedLeader && haveEnoughVotes(voterIDtoVote, getCurrentVote())) {
                                //first check if there are any new votes for a higher ranked possible leader before I declare a leader
                                while ((msg = incomingMessages.poll(Util.finalizeWait, TimeUnit.MILLISECONDS)) != null) {
                                    vote = getNotificationFromMessage(msg);
                                    if (vote == null) continue;
                                    if (!vote.getState().equals(OBSERVER) && supersedesCurrentVote(vote.getProposedLeaderID(), vote.getPeerEpoch())) {
                                        // higher vote found: continue in my election loop
                                        incomingMessages.put(msg);
                                        break;
                                    }
                                }
                                //If not, set my own state to either LEADING or FOLLOWING and exit the election
                                if (msg == null) {
                                    return acceptElectionWinner(getCurrentElectionNotification());
                                }
                            }
                            // continue looping on the election loop
                            break;
                        //if the sender is following a leader already or thinks it is the leader
                        case FOLLOWING:
                        case LEADING:
                            //IF: see if the sender's vote allows me to reach a conclusion based on the election epoch that I'm in, i.e. it gives the majority to the vote of the FOLLOWING or LEADING peer whose vote I just received.
                            if (vote.getPeerEpoch() == proposedEpoch) {
                                // accept the election winner.
                                //As, once someone declares a winner, we are done. We are not worried about / accounting for misbehaving peers.
                                logger.finer("accepting vote from " + vote.getState().name() + " peer " + vote.getSenderID());
                                return acceptElectionWinner(vote);
                            }
                            //ELSE: if n is from a LATER election epoch
                            else if (vote.getPeerEpoch() > proposedEpoch) {
                                //IF a quorum from that epoch are voting for the same peer as the vote of the FOLLOWING or LEADING peer whose vote I just received.
                                if (haveEnoughVotes(voterIDtoVote, vote)) {
                                    // accept their leader, and update my epoch to be their epoch
                                    setProposal(vote.getProposedLeaderID(), vote.getPeerEpoch());
                                    return acceptElectionWinner(vote);
                                }
                                //else, keep looping on the election loop.
                            }
                            break;
                        case OBSERVER:
                            // messages from observers are ignored

                            // keep track of observers in the peer server
                            ((ZooKeeperPeerServerImpl) this.myPeerServer).addObserverToMap(vote.getSenderID(), msg.getSenderAddress());
                            break;
                    }
                }
            } catch(InterruptedException e){
                e.printStackTrace();
                logger.log(Level.WARNING, "InterruptedException thrown in leader election thread", e);
            }
        } // end while loop
        return null;
    }

    private void sendNotifications() {
        logger.info("Sending vote: " + getCurrentVote() + " from server #" + myPeerServer.getServerId());
        myPeerServer.sendBroadcast(Message.MessageType.ELECTION, getCurrentElectionNotification().getContentBytes());
    }

    private ElectionNotification getCurrentElectionNotification() {
        return new ElectionNotification(myPeerServer.getPeerState(), getCurrentVote(), myPeerServer.getServerId());
    }

    private Vote acceptElectionWinner(ElectionNotification vote) {
        // set my current leader to the winning vote/server
        // leader is set in ZookeeperLeaderPeerServerImpl.run()

        // change my state to LEADING if I won the election
        if (vote.getProposedLeaderID() == myPeerServer.getServerId()) {
            myPeerServer.setPeerState(LEADING);
        }
        // change my state to FOLLOWING if I didn't win the election
        else if (myPeerServer.getPeerState() != OBSERVER){
            myPeerServer.setPeerState(FOLLOWING);
        }
        logger.info("Accepting Server #" + vote.getProposedLeaderID() + " as the leader");
        //clear out the incoming queue before returning
        incomingMessages.clear();
        return vote;
    }

    /*
     * We return true if one of the following three cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     * 3- current vote is for myself but I am an OBSERVER, therefore all votes from LOOKING peers supercede me
     */
    protected boolean supersedesCurrentVote(long newId, long newEpoch) {
        return (newEpoch >= this.proposedEpoch && newId > this.proposedLeader)
                // added third condition: see (3) above
                || (myPeerServer.getPeerState() == OBSERVER && this.proposedLeader == myPeerServer.getServerId());
    }

    /**
     * Termination predicate. Given a set of votes, determines if have sufficient support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one current vote
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification> votes, Vote proposal) {
        //is the number of votes for the proposal >= the size of my peer server’s quorum?
        if (proposal == null || votes == null) return false;
        int counter = 0;
        for (ElectionNotification vote : votes.values()) {
            if (vote.getProposedLeaderID() == proposal.getProposedLeaderID() && vote.getPeerEpoch() >= proposal.getPeerEpoch()) {
                logger.finer("vote " + vote + " from server #" + vote.getSenderID() + " is equal to proposal " + proposal);
                counter++;
            }
            //return once counter is >= the quorum
            if (counter >= this.myPeerServer.getQuorumSize()) {
                logger.info("There are " + counter + " votes counting toward a quorum size of at least "
                        + myPeerServer.getQuorumSize() + " for proposed vote " + proposal);
                return true;
            }
        }
        //if method hasn't returned yet, then proposal is denied
        return false;
    }
}
