package edu.yu.cs.com3800;

import java.nio.ByteBuffer;

/**
 * Used to communicate votes across servers.
 * Adds information to the Vote about the server casting the vote.
 */
public class ElectionNotification extends Vote {
    //ID of the sender
    final private long senderID;
    //state of the sender
    final private ZooKeeperPeerServer.ServerState state;

    public ElectionNotification(long proposedLeaderID, ZooKeeperPeerServer.ServerState state, long senderID, long peerEpoch) {
        super(proposedLeaderID, peerEpoch);
        this.senderID = senderID;
        this.state = state;
    }

    public ElectionNotification(ZooKeeperPeerServer.ServerState state, Vote vote, long senderID) {
        super(vote.getProposedLeaderID(), vote.getPeerEpoch());
        this.senderID = senderID;
        this.state = state;
    }

    public byte[] getContentBytes() {
        /*
        size of buffer =
        1 long (proposedLeaderID) = 8 bytes
        1 char (state type) = 2 bytes
        1 long (sender ID) = 8 bytes
        1 long (peerEpoch) = 8 bytes
         = 26
         */
        int bufferSize = 26;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        buffer.clear();
        buffer.putLong(getProposedLeaderID());
        buffer.putChar(this.state.getChar());
        buffer.putLong(this.senderID);
        buffer.putLong(getPeerEpoch());
        buffer.flip();
        return buffer.array();
    }

    public long getSenderID() {
        return senderID;
    }

    public ZooKeeperPeerServer.ServerState getState() {
        return state;
    }

    public boolean equals(Object other) {
        if (other == null) return false;
        if (!super.equals(other)) {
            return false;
        }
        if (!(other instanceof ElectionNotification)) {
            return false;
        }
        ElectionNotification otherEN = (ElectionNotification) other;
        return this.senderID == otherEN.senderID && this.state == otherEN.state;
    }
}