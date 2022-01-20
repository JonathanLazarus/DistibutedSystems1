package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.Vote;
import edu.yu.cs.com3800.ZooKeeperPeerServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;

// ZooKeeperCluster - cluster without a gateway server or gateway peer server
public class ZKCluster {
    // List of ZK servers that do not observe, ie the servers that make up the master-worker cluster
    private ArrayList<ZooKeeperPeerServerImpl> clusterServers;
    // List of observing servers
    private ArrayList<ZooKeeperPeerServerImpl> observingServers;
    // complete mapping of all servers including master-worker peers and gateway observing peers
    private HashMap<Long, InetSocketAddress> peerIDtoAddress;

    private int currPort = 4000;

    // creates a cluster without a gateway (only runs zookeeper leader election)
    public ZKCluster(int nonObservers, int observers) throws IOException {
        checkInitialSize(nonObservers, observers);
        // initialize the instance variable map
        initMap(nonObservers, observers);
        // initialize server lists with peer servers
        createServers(nonObservers, observers);
    }

    private void checkInitialSize(int nonObservers, int observers) {
        if (nonObservers < 2) throw new IllegalArgumentException("cannot have a cluster of less than 2 active members");
        if (this instanceof GatewayCluster && observers < 1)
            throw new IllegalArgumentException("cluster with gateway must contain at least one observer");
        else if (observers < 0) throw new IllegalArgumentException("cannot have negative amount of observers");
    }

    public ArrayList<ZooKeeperPeerServerImpl> getObservers() {
        return this.observingServers;
    }

    public ArrayList<ZooKeeperPeerServerImpl> getClusterServers() {
        return this.clusterServers;
    }

    // returns the amount of total peers in the cluster. Includes amount of observers in sum.
    public int size() {
        return clusterSize() + observerSize();
    }

    // returns amount of peer servers in the master-worker cluster (non-observers)
    public int clusterSize() {
        return clusterServers.size();
    }

    // returns amount of observing gateway peer servers in the cluster
    public int observerSize() {
        return observingServers.size();
    }

    // starts the cluster
    public void start() {
        for (ZooKeeperPeerServerImpl observer : this.observingServers) {
            observer.start();
        }
        for (ZooKeeperPeerServerImpl clusterServer : this.clusterServers) {
            clusterServer.start();
        }
    }

    // shuts cluster down
    public void shutdown() {
        for (ZooKeeperPeerServerImpl observer : this.observingServers) {
            observer.shutdown();
        }
        for (ZooKeeperPeerServerImpl clusterServer : this.clusterServers) {
            clusterServer.shutdown();
        }
    }

    public void waitForElection() {
        while (clusterServers.get(0).getCurrentLeader() == null && clusterServers.get(1).getCurrentLeader() == null) {
            // wait
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void printLeaders() {
        printLeaders(clusterServers);
        printLeaders(observingServers);
    }

    private void printLeaders(ArrayList<ZooKeeperPeerServerImpl> L) {
        for (ZooKeeperPeerServer server : L) {
            Vote leader = server.getCurrentLeader();
            if (leader != null) {
                System.out.println(server.getClass().getSimpleName() + " on port " + server.getMyAddress().getPort() + " whose ID is " + server.getServerId() + " has the following ID as its leader: " + leader.getProposedLeaderID() + " and its state is " + server.getPeerState().name());
            }
        }
    }

    public long getHighestIdParticipant() {
        return this.clusterServers.get(clusterServers.size()-1).getServerId();
    }

    // N is amount of non-observers in the cluster. G is amount of observers
    private void initMap(int N, int G) {
        int maxSize = 25; // modifiable
        if (N + G > maxSize) throw new IllegalArgumentException("cluster size should be less than " + maxSize);
        //create IDs and addresses
        this.peerIDtoAddress = new HashMap<>(N + G);
        // i = 1 because we start server IDs with 1
        for (int i = 1; i <= N+G; i++) {
            peerIDtoAddress.put(Integer.valueOf(i).longValue(), new InetSocketAddress("localhost", getNextPort()));
        }
    }

    private void createServers(int N, int G) {
        //create master-worker cluster servers
        this.clusterServers = new ArrayList<>(N);
        for (int i = 1; i <= N; i++) {
            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) this.peerIDtoAddress.clone();
            // remove server's own id->address mapping from the map
            InetSocketAddress peerAddress = map.remove(Integer.valueOf(i).longValue());
            // initialize each peer server in the M-W cluster with a peer ID Map which includes observers [@118]
            ZooKeeperPeerServerImpl server =
                    new ZooKeeperPeerServerImpl(peerAddress.getPort(), 0, Integer.valueOf(i).longValue(), map, G);
            this.clusterServers.add(server);
            // servers are started via call Cluster.start()
        }

        // create OBSERVERS/gateway server(s)
        if (G > 0) {
            this.observingServers = new ArrayList<>(G);
            for (int i = N+1; i <= N+G; i++) {
                HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) this.peerIDtoAddress.clone();
                // remove server's own id->address mapping from the map
                InetSocketAddress peerAddress = map.remove(Integer.valueOf(i).longValue());
                // initialize each gateway peer server in the M-W cluster with a peer ID Map which includes observers [@118]
                GatewayPeerServerImpl server =
                        new GatewayPeerServerImpl(peerAddress.getPort(), 0, Integer.valueOf(i).longValue(), map, G);
                this.observingServers.add(server);
                // servers are started via call Cluster.start()
            }
        }
    }

    protected int getNextPort() {
        int ret = this.currPort;
        this.currPort += 10;
        return ret;
    }
}
