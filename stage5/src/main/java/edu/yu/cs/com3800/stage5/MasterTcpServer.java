package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MasterTcpServer extends Thread implements LoggingServer {
    private final int tcpPort;
    private Logger logger;
    private final ZooKeeperPeerServerImpl myPeer;
    private final LinkedBlockingQueue<TcpWrapper> tcpRequests;

    public MasterTcpServer(ZooKeeperPeerServerImpl peerServer, LinkedBlockingQueue<TcpWrapper> tcpRequests) {
        this.myPeer = peerServer;
        this.tcpPort = peerServer.getUdpPort()+2;
        this.tcpRequests = tcpRequests;
        try {
            this.logger = initializeLogging(this.getClass().getSimpleName() + "-on-TCP-port-" + tcpPort);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        logger.finest("run() invoked");
        // loop until the thread is shut down - accepting all connections over the network
        while(!this.isInterrupted() && !myPeer.isShutdown()) {
            try (ServerSocket serverSocket = new ServerSocket(this.tcpPort)) {
                logger.info("Server is up and ready for client connections");
                while (!this.isInterrupted() && !myPeer.isShutdown()) {
                    // wait for request from a client
                    Socket clientSocket = serverSocket.accept();
                    logger.fine("received new request");

                    // pre-process the request
                    InputStream in = clientSocket.getInputStream();
                    OutputStream out = clientSocket.getOutputStream();

                    logger.finest("reading message from network");
                    Message msg = new Message(Util.readAllBytesFromNetwork(in));

                    // check that the message (from client) is of type WORK to send to RRL thread
                    if (msg.getMessageType() == Message.MessageType.WORK) {
                        // send along to RRL to finish processing request synchronously
                        this.tcpRequests.offer(new TcpWrapper(out, msg));
                        logger.fine("sent request #" + msg.getRequestID() + " to RRL");
                    } else {
                        logger.info("Message type was not of type WORK");
                        msg.setErrorOccurred(true);
                        // return bad message
                        out.write(msg.getNetworkPayload());
                        out.close();
                    }
                }
            } catch (BindException e) {
                // wait for javaRunnerFollower to stop using its server with this address
                continue;
            } catch (IOException e) {
                logger.log(Level.WARNING, "issue with server socket on peer [" + myPeer.getServerId() + "]", e);
            }
        }
        logger.severe("exiting MasterTCPServer.run()");
    }

    // TCP request wrapper
    public static class TcpWrapper {
        public OutputStream getOut() {
            return out;
        }

        public Message getMsg() {
            return msg;
        }

        private final OutputStream out;
        private final Message msg;

        public TcpWrapper(OutputStream out, Message msg) {
            this.out = out;
            this.msg = msg;
        }
    }
}
