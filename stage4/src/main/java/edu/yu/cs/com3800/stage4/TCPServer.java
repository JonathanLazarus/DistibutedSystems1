package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.io.*;
import java.net.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TCPServer extends Thread implements LoggingServer {
    private int tcpPort;
    private Logger logger;
    private LinkedBlockingQueue<Message> tcpRequests;
    private LinkedBlockingQueue<Message> tcpResponses;

    public TCPServer(int tcpPort, LinkedBlockingQueue<Message> tcpRequests, LinkedBlockingQueue<Message> tcpResponses) throws IOException {
        this.tcpPort = tcpPort;
        this.logger = initializeLogging(this.getClass().getSimpleName() + "-on-TCP-port-" + tcpPort);
        this.tcpRequests = tcpRequests;
        this.tcpResponses = tcpResponses;
    }

    @Override
    public void run() {
        logger.finest("run() invoked");
        try (ServerSocket serverSocket = new ServerSocket(this.tcpPort);)
        {
            // loop until the thread is shut down - accepting all connections over the network
            while (!this.isInterrupted()) {
                // wait for request from a client
                Socket clientSocket = serverSocket.accept();
                InputStream in = clientSocket.getInputStream();
                OutputStream out = clientSocket.getOutputStream();

                Message msg = new Message(Util.readAllBytesFromNetwork(in));
                logger.info(msg.toString());
                //only send WORK to RoundRobinLeader
                if (msg.getMessageType() == Message.MessageType.WORK) {
                    logger.finest("sent received message from TCP client to the cluster server");
                    this.tcpRequests.add(msg);
                }

                // wait for response from follower - Synchronously
                int timeOut = Util.finalizeWait;
                msg = null;
                // first get work from client/completed work from workers
                while (msg == null) {
                    try {
                        //Remove next notification from queue
                        msg = tcpResponses.poll(timeOut, TimeUnit.MILLISECONDS);
                        //if no notifications received
                        // implement exponential back-off when notifications not received
                        timeOut = Math.min(timeOut * 2, Util.maxNotificationInterval);
                    } catch (InterruptedException e) {
                        logger.log(Level.WARNING, "InterruptedException caught while waiting for response", e);
                    }
                }
                logger.finest("received message. Now sending back to Round Robin Leader");
                out.write(msg.getNetworkPayload());

                // client socket has been closed
                clientSocket.close();
            }
        } catch (IOException e) {
            logger.log(Level.WARNING, "issue starting server socket", e);
        }

    }
}
