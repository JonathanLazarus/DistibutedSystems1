package edu.yu.cs.com3800;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Util {
    public static int GOSSIP = 3000;
    public static int FAIL = GOSSIP * 6; //10
    public static int CLEANUP = FAIL * 2;

    /**
     * Upper bound on the amount of time between two consecutive notification checks.
     * This impacts the amount of time to get the system up again after long partitions. Currently, 60 seconds.
     */
    public final static int maxNotificationInterval = 60000;
    public static final int finalizeWait = 200;


    public final static String HTTPENDPOINT = "/compileandrun";

    /**
     * Use for testing only.
     *
     * @param gossip amount of time to wait to send gossip
     * @param failGossipMultiplier amount to multiply GOSSIP by to set FAIL interval
     *                             Larger values will ensure less heartbeat time-outs,
     *                             while smaller values will induce more heartbeat time-outs.
     * @param cleanupFailMultiplier amount to multiply FAIL interval by
     */
    public static void setGossip(int gossip, int failGossipMultiplier, int cleanupFailMultiplier) {
        GOSSIP = gossip;
        FAIL = GOSSIP * failGossipMultiplier;
        CLEANUP = FAIL * cleanupFailMultiplier;
    }

    public static byte[] readAllBytesFromTcp(InputStream in, Socket socket) throws IOException {
        while (in.available() == 0) {
            if (socket.isClosed()) {
                return null;
            }
            try {
                Thread.currentThread().sleep(500);
            }
            catch (InterruptedException e) {
            }
        }
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int numberRead;
        byte[] data = new byte[40960];
        while (in.available() > 0 && (numberRead = in.read(data, 0, data.length)) != -1   ) {
            buffer.write(data, 0, numberRead);
        }
        return buffer.toByteArray();
    }

    public static byte[] readAllBytesFromNetwork(InputStream in)  {
        try {
            while (in.available() == 0) {
                try {
                    Thread.currentThread().sleep(500);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        catch(IOException e){
            e.printStackTrace();
        }
        return readAllBytes(in);
    }

    public static byte[] readAllBytes(InputStream in) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int numberRead;
        byte[] data = new byte[40960];
        try {
            while (in.available() > 0 && (numberRead = in.read(data, 0, data.length)) != -1   ) {
                buffer.write(data, 0, numberRead);
            }
        }catch(IOException e){}
        return buffer.toByteArray();
    }

    public static Thread startAsDaemon(Thread run, String name) {
        Thread thread = new Thread(run, name);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    public static String getStackTrace(Exception e){
        ByteArrayOutputStream bas = new ByteArrayOutputStream();
        PrintStream myErr = new PrintStream(bas,true);
        e.printStackTrace(myErr);
        myErr.flush();
        myErr.close();
        return bas.toString();
    }

    public static void logList(Logger logTo, Level level, List<Long> list) {
        StringBuilder sb = new StringBuilder("\nList: Server ID#");
        if (logTo == null || list == null) {
            return;
        }
        for (Long id : list) {
            sb.append("\n\tServer #").append(id);
        }
        logTo.log(level, sb.toString());
    }

    public static void logMap(Logger logTo, Level level, Map<Long, InetSocketAddress> map) {
        StringBuilder sb = new StringBuilder("\nMap: Server ID# to address port");
        if (logTo == null || map == null) {
            return;
        }
        for (Map.Entry<Long, InetSocketAddress> entry : map.entrySet()) {
            sb.append("\n\tServer #").append(entry.getKey()).append(" is mapped to port ").append(entry.getValue().getPort());
        }
        logTo.log(level, sb.toString());
    }

    public static void logAndPrint(Logger logger, Level level, String log) {
        logger.log(level, log);
        System.out.println(log);
    }
}
