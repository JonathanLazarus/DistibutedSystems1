package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
                if (!file.isDirectory()) file.delete();
            }
        }
    }


    @After
    public void teardown() {
        if (cluster != null) cluster.shutdown();
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
        try {
            cluster = new GatewayCluster(5, 2);
            client = new Client(cluster.getGateway().getServerAddress(), Util.HTTPENDPOINT, false);
            cluster.start();
            cluster.waitForElection();
            System.out.println("sending request from client from test code");
            for (int i = 0; i < 20; i++) {
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