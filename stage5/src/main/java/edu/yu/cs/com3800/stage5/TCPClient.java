package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.ZooKeeperPeerServer;

import java.io.*;
import java.net.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TCPClient {
    private final String serverHostname;
    private final int serverPort;
    private final String targetState;

    Logger logger;

    public TCPClient(Logger logger, String serverHostname, int serverPort, ZooKeeperPeerServer.ServerState targetState) {
        this.logger = logger;
        this.serverHostname = serverHostname;
        this.serverPort = serverPort;
        this.targetState = targetState.name();
    }

    /**
     * sends message via TCP to address which this class was initialized with
     * @param msg message to send to host
     * @return response, signified by COMPLETED_WORK. Or returns the original msg if
     * an error was encountered while servicing the request
     */
    public Message sendMessageToHost(Message msg) {
        try (Socket hostSocket = new Socket(serverHostname, serverPort);
             InputStream in = hostSocket.getInputStream();
             OutputStream out = hostSocket.getOutputStream())
        {
            logger.fine("sending message [" + msg.getRequestID() + "] to " + targetState + " peer on TCP port " + serverPort);
            out.write(msg.getNetworkPayload());
            logger.fine("Reading response from network");
            if (hostSocket.isClosed()) throw new SocketException("socket was closed before we were finished using it");
            Message received = new Message(Util.readAllBytesFromTcp(in, hostSocket));
            logger.fine("Received response with ID [" + received.getRequestID() + "] from " + targetState + " peer: " + new String(received.getMessageContents()));
            hostSocket.close();
            logger.finer("closed TCP socket connection to " + targetState + " peer on port " + serverPort);
            return received;
        } catch (IllegalArgumentException e) {
            // IAE thrown by Message constructor
            if (e.getMessage().equals("initiated Message with null networkPayLoad")) {
                logger.fine("hostSocket was closed while reading bytes from network");
                return msg; // return original msg to requeue
            }
            else throw e; // rethrow
        } catch (IOException e) {
            logger.log(Level.INFO, e.getClass().getSimpleName() + " caught while waiting for response from leader to request [" + msg.getRequestID() + "]");
            logger.fine("returning original message [" + msg.getRequestID() + "] back sender for further correct processing");
            return msg;
        }
    }

}
