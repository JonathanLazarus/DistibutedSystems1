package edu.yu.cs.com3800.stage4;

import com.sun.net.httpserver.*;
import edu.yu.cs.com3800.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GatewayServer extends Thread implements LoggingServer {
    private HttpServer server;
    private GatewayPeerServerImpl observer;
    private final int myPort;

    private Logger logger;

    private ThreadLocal<Logger> localLogger;

    public GatewayServer(int port, GatewayPeerServerImpl gatewayPeerServer) throws IOException {
        this.myPort = port;
        this.server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
        this.server.createContext(Util.HTTPENDPOINT, new MyHandler());
        this.server.setExecutor(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors())); //todo *2 @130
        this.observer = gatewayPeerServer;
        this.logger = initializeLogging(this.getClass().getSimpleName() + "-on-port-" + port);
    }

    public InetSocketAddress getServerAddress() {
        return this.server.getAddress();
    }

    public void shutdown() {
        logger.info("shutdown called for GatewayPeerServerImpl");
        this.server.stop(0);
        this.observer.shutdown();
        interrupt();
    }

    @Override
    public void run() {
        // start server thread that accepts client requests
        this.server.start();
        // observe leader election results
        Util.startAsDaemon(observer, "Gateway observer thread");
    }

    private InetSocketAddress getCurrentLeader() {
        return this.observer.getLeaderAddress();
    }

    /** This handler handles POST requests to the path “/compileandrun”
     *  and sends request to the Master-Worker cluster for processing
     *  It must make sure that the content type in the client request
     *  is “text/x-java-source” and return a response code of 400 if it is not.
     */
    public class MyHandler implements HttpHandler, LoggingServer {

        /**
         * Handle the given request and generate an appropriate response.
         * See {@link HttpExchange} for a description of the steps
         * involved in handling an exchange.
         *
         * @param exchange the exchange containing the request from the
         *                 client and used to send the response
         * @throws NullPointerException if exchange is <code>null</code>
         */
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (localLogger == null) localLogger = new ThreadLocal<>();
            if (localLogger.get() == null) localLogger.set(initializeLogging("HTTPServerHandler-port-" + myPort + "-thread-" + currentThread().getId()));
            Logger logger = localLogger.get();
            logger.finest("handling exchange");
            // Handles only POST requests

            if (exchange.getRequestMethod().equals("POST")) {
                logger.finest("POST exchange");
                if (exchange.getRequestHeaders().getFirst("Content-Type").equals("text/x-java-source")) {
                    // send request to cluster
                    try {
                        // read the exchange contents/body
                        InputStream is = exchange.getRequestBody();
                        String text = new String(Util.readAllBytesFromNetwork(is));
                        logger.finest("body of HTTP request: \"" + text + "\'");

                        // wait for leader election to finish
                        InetSocketAddress leader = getCurrentLeader();
                        while (leader == null) {
                            Thread.sleep(500);
                            leader = getCurrentLeader();
                        }
                        int leaderTcpPort = leader.getPort() +2;


                        // construct a Message to send to the cluster over TCP
                        Message msg = new Message(Message.MessageType.WORK, text.getBytes(), "localhost", myPort, //Util.tcpServerHostname, Util.tcpServerPort);
                                leader.getHostName(), leaderTcpPort);

                        // send the Message via TCP client
                        logger.finer("opening TCP connection via socket on port " + leaderTcpPort);
                        Socket hostSocket = new Socket("localhost", leaderTcpPort);
                        InputStream in = hostSocket.getInputStream();
                        OutputStream out = hostSocket.getOutputStream();
                        out.write(msg.getNetworkPayload());
                        logger.finest("sent message to TCP port " + leaderTcpPort);
                        Message response = new Message(Util.readAllBytesFromNetwork(in));
                        hostSocket.close();
                        logger.finer("closed socket");
                        String output = new String(response.getMessageContents());

                        logger.finest("response from TCPServer (" + leaderTcpPort + "): " + output);

                        // send response back to httpClient
                        int responseCode = response.getErrorOccurred() ? 400 : 200;
                        exchange.sendResponseHeaders(responseCode, output.getBytes(StandardCharsets.UTF_8).length);
                        out = exchange.getResponseBody();
                        out.write(output.getBytes());
                        out.close();

                    } catch (UnknownHostException e) {
                        logger.log(Level.WARNING, "Unknown host exc thrown by TCP client", e);
                    } catch (IOException e) {
                        logger.log(Level.WARNING, "IOE thrown by TCP client", e);
                    } catch (InterruptedException e) {
                        logger.log(Level.WARNING, "Interrupted Exception caught while waiting for leader", e);
                    }
                } else {
                    logger.finest("exchange Content-Type was not text/x-java-source\nSending 400 response");
                    exchange.sendResponseHeaders(400, 0);
                }
            } else {
                logger.finest("was not a POST request\nSending 400 response");
                byte[] onlyPost = "Only POST is supported\n".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(400, onlyPost.length);
                OutputStream out = exchange.getResponseBody();
                out.write(onlyPost);
                out.close();
            }
        }
    }
}













