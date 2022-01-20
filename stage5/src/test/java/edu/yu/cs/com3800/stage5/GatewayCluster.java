package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.util.ArrayList;

public class GatewayCluster extends ZKCluster {
    // GatewayServer
    private GatewayServer gateway;

    //creates a cluster with a gateway
    public GatewayCluster(int nonObservers, int observers) throws IOException {
        super(nonObservers, observers); // do initial checks and initialize lists and map

        this.gateway = new GatewayServer(getNextPort(), getGatewayPeerServer());
    }

    public GatewayServer getGateway() {
        return this.gateway;
    }

    public int getGatewayPort() {
        return this.gateway.getServerAddress().getPort();
    }

    public GatewayPeerServerImpl getGatewayPeerServer() {
        return (GatewayPeerServerImpl) this.getObservers().get(0);
    }

    @Override
    public void start() {
        this.gateway.start();
        ArrayList<ZooKeeperPeerServerImpl> observers = getObservers();
        for (int i = 1; i < observerSize(); i++) {
            // we skip index 0 because that is started in gateway thread above
            observers.get(i).start();
        }
        for (ZooKeeperPeerServerImpl clusterServer : getClusterServers()) {
            clusterServer.start();
        }
    }

    @Override
    public void shutdown() {
        System.out.println("shutdown called on GatewayCluster");
        this.gateway.shutdown();
        for (ZooKeeperPeerServerImpl observer : getObservers()) {
            observer.shutdown();
        }
        for (ZooKeeperPeerServerImpl clusterServer : getClusterServers()) {
            clusterServer.shutdown();
        }
    }

    @Override
    public void waitForElection() {
        super.waitForElection();
        while(getGatewayPeerServer().getCurrentLeader() == null) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
