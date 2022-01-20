package edu.yu.cs.com3800.stage5;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.logging.Logger;

public class GatewayPeerServerImpl extends ZooKeeperPeerServerImpl {
    private Logger logger;

    public GatewayPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long, InetSocketAddress> peerIDtoAddress, Long gatewayObserverID, int observers) {
        super(myPort, peerEpoch, id, peerIDtoAddress, gatewayObserverID, observers);
        super.setPeerState(ServerState.OBSERVER);
        this.logger = super.getLogger();
        logger.info("logger initialized in derived class");
    }

    // Always in OBSERVER state
    @Override
    public ServerState getPeerState() {
        return ServerState.OBSERVER;
    }

    @Override
    public void setPeerState(ServerState newState) {
        // do nothing because gateway peer server can only be OBSERVER
        return;
    }
}
