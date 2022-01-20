package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.io.*;
import java.net.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TCPClient {
    String serverHostname;
    int serverPort;

    Logger logger;

    public TCPClient(Logger logger, String serverHostname, int serverPort) {
        this.logger = logger;
        this.serverHostname = serverHostname;
        this.serverPort = serverPort;
    }

    public Message sendMessageToHost(Message msg) throws ConnectException {
        try (Socket hostSocket = new Socket(serverHostname, serverPort);
             InputStream in = hostSocket.getInputStream();
             OutputStream out = hostSocket.getOutputStream();)
        {
            logger.fine("sending message to WORKER on TCP port " + serverPort);
            out.write(msg.getNetworkPayload());
            logger.fine("Reading response from network");
            Message received = new Message(Util.readAllBytesFromNetwork(in));
            logger.fine("Received message from WORKER: " + new String(received.getMessageContents()));
            hostSocket.close();
            logger.finer("closed TCP socket connection to WORKER on port " + serverPort);
            return received;
        } catch (UnknownHostException e) {
            logger.log(Level.WARNING, "Unknown host exc thrown by TCP client", e);
        } catch (ConnectException e) {
            // special case described in ZooKeeperPeerServer.getFollowerMapping()
            logger.log(Level.WARNING, "No server accepting connections on provided TCP port "
                    + msg.getReceiverPort(), e);
            // re-throw ConnectionException to signify that this port is not a follower and should be removed from RRL
            throw e;
        } catch (IOException e) {
            logger.log(Level.WARNING, "IOE thrown by TCP client", e);
        }
        return null;
    }

}
