package edu.yu.cs.com3800.stage5;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public class ZooKeeperPeerServerImplTest {
    private ZKCluster cluster;

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
        this.cluster.shutdown();
    }

    @Test
    public void observerWithHighestIDLoses() {
        try {
            cluster = new ZKCluster(5,1);
            cluster.start();
            //wait for election to finish
            Thread.sleep(2000);
            ZooKeeperPeerServerImpl observer = cluster.getObservers().get(0);
            assertTrue(observer.getCurrentLeader().getProposedLeaderID() < observer.getServerId());
            cluster.printLeaders();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void manyObservers() {
        try {
            cluster = new ZKCluster(3,4);
            cluster.start();
            //wait for election to finish
            Thread.sleep(3000);
            cluster.printLeaders();
            ZooKeeperPeerServerImpl observer = cluster.getObservers().get(3);
            assertEquals("Highest ID lost", cluster.getHighestIdParticipant(), observer.getCurrentLeader().getProposedLeaderID());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            fail();
        }
    }
}