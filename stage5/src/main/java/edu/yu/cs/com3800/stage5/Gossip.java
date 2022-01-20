package edu.yu.cs.com3800.stage5;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Gossip {
    private final long senderId;
    private final long timeReceived;
    private final HashMap<Long, Long> table; // idToHeartbeat map of sender

    public Gossip(byte[] contents) {
        this.timeReceived = System.currentTimeMillis();
        this.table = new HashMap<>();
        ByteBuffer msgBytes = ByteBuffer.wrap(contents);
        this.senderId = msgBytes.getLong();
        while (msgBytes.hasRemaining()) {
            long k = msgBytes.getLong();
            long v = msgBytes.getLong();
            table.put(k, v);
        }
    }

    public Map<Long, Long> getTable() {
        return Collections.unmodifiableMap(table);
    }

    public Long getHeartbeat(long id) {
        return table.get(id);
    }

    public long getTimeReceived() {
        return timeReceived;
    }

    public long getSenderId() {
        return senderId;
    }
}
