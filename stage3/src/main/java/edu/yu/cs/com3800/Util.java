package edu.yu.cs.com3800;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

public class Util {
    public final static int finalizeWait = 200;

    /**
     * Upper bound on the amount of time between two consecutive notification checks.
     * This impacts the amount of time to get the system up again after long partitions. Currently, 60 seconds.
     */
    public final static int maxNotificationInterval = 60000;

    public static byte[] readAllBytesFromNetwork(InputStream in)  {
        try {
            while (in.available() == 0) {
                try {
                    Thread.currentThread().sleep(500);
                }
                catch (InterruptedException e) {
                }
            }
        }
        catch(IOException e){}
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
}
