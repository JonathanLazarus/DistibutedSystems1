package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class GatewayServerTest {
    private Client client;
    private GatewayCluster cluster;

    @Before
    public void startup() {
        //delete previous log files
        File f = new File(System.getProperty("user.dir") + "/logs");
        if (f.exists()) {
            for (File file : f.listFiles()) {
                //if (!file.isDirectory()) file.delete();
                file.delete();
            }
        }
    }


    @After
    public void teardown() {
        System.out.println("shutting down cluster in @After");
        if (cluster != null) cluster.shutdown();
    }

    @Test
    public void gatewayQueuesRequestsDuringLeaderFailure() {
        try {
            Util.setGossip(3000, 10, 2);
            cluster = new GatewayCluster(3, 1);
            cluster.start();
            GatewayPeerServerImpl observer = cluster.getGatewayPeerServer();
            cluster.waitForElection();
            // create an asyncClient
            this.client = new Client(cluster.getGateway().getServerAddress(), Util.HTTPENDPOINT, true);
            // send 6 requests
            System.out.println("sending 6 requests");
            this.client.sendConcurrentRequests(6);
            // now "kill" leader, ie make the gateway think leader is dead by setting leader to null in observer
            Thread.sleep(1000);
            // save leader so we can reset later
            Vote leader = observer.getCurrentLeader();
            assertNotNull("leader was null", leader);
            observer.setCurrentLeader(null);
            Thread.sleep(1000);

            // now send 8 requests
            System.out.println("Sending 8 requests when no leader");
            List<CompletableFuture<Client.Response>> asyncResponses =
                    this.client.sendRequestsAndGetAsyncList(8);
            Thread.sleep(1000);
            // check that the requests are queued, and haven't been sent
            assertEquals("There were not enough queued requests", 8, cluster.getGateway().getQueuedRequestsSize());
            // now reinstate leader
            observer.setCurrentLeader(leader);
            Thread.sleep(1000);
            this.client.printAsyncResponses(asyncResponses);
            // check that all requests previously in queue have been sent to leader
            assertEquals("At least one request was not sent to leader", 0, cluster.getGateway().getQueuedRequestsSize());
            return;
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void httpServerIsSynchronous() {
        try {
            cluster = new GatewayCluster(5, 2);
            client = new Client(cluster.getGateway().getServerAddress(), Util.HTTPENDPOINT, false);
            cluster.start();
            cluster.waitForElection();
            cluster.printLeaders();
            System.out.println("sending request from client from test code");
            for (int i = 0; i < 20; i++) {
                Client.Response response = client.sendCompileAndRunRequest(TestUtil.getVersionedValidClass(i));
                assertEquals(200, response.getCode());
                assertEquals("Hello world! from code version " + i, response.getBody());
            }
            // now check that the server processes requests synchronously. We can tell if multiple log files were created
            long count;
            try (Stream<Path> walk = Files.walk(Paths.get(System.getProperty("user.dir") + "/logs"), 1)) {
                count = walk
                        .filter(Files::isReadable)
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().matches("HTTPServerHandler-port-\\d+-thread-\\d+.log"))
                        .count();
            }
            Thread.sleep(1000);
            assertTrue("HttpServer did not process requests synchronously. File count: " + Long.valueOf(count).intValue() + " was less than processor count: " + Runtime.getRuntime().availableProcessors(),
                    Long.valueOf(count).intValue() >= Runtime.getRuntime().availableProcessors());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void gateway_to_master_TCP_Synchronous_Client() {
        Util.setGossip(2500, 10, 2);
        try {
            int n = 4; // non-observers
            cluster = new GatewayCluster(n, 1);
            client = new Client(cluster.getGateway().getServerAddress(), Util.HTTPENDPOINT, false);
            cluster.start();
            cluster.waitForElection();
            System.out.println("sending request from client from test code");
            for (int i = 0; i < 40; i++) {
                if (i == 6) {
                    cluster.getClusterServers().get(n-1).shutdown();
                }
                Client.Response response = client.sendCompileAndRunRequest(TestUtil.getVersionedValidClass(i));
                System.out.println(response.getBody());
                assertEquals(200, response.getCode());
                assertEquals("Hello world! from code version " + i, response.getBody());
            }
            cluster.printLeaders();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void concurrent_client() {
        try {
            cluster = new GatewayCluster(5, 1);
            client = new Client(cluster.getGateway().getServerAddress(), Util.HTTPENDPOINT, true);
            cluster.start();
            cluster.waitForElection();
            cluster.printLeaders();
            System.out.println("starting asynchronous client");
            client.sendConcurrentRequests(30);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }
}