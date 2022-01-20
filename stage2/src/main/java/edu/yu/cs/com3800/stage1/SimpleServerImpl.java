package edu.yu.cs.com3800.stage1;

import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.SimpleServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import com.sun.net.httpserver.*;
import edu.yu.cs.com3800.Util;

public class SimpleServerImpl implements SimpleServer {
    private HttpServer server;

    public SimpleServerImpl(int port) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.createContext("/compileandrun", new MyHandler());
        this.server.setExecutor(null); // creates a default executor
    }
    /**
     * start the server
     */
    @Override
    public void start() {
        this.server.start();
    }

    /**
     * stop the server
     */
    @Override
    public void stop() {
        this.server.stop(0);
    }

    public static void main(String[] args) {
        int port = 9000;
        if(args.length >0) {
            port = Integer.parseInt(args[0]);
        }
        SimpleServer myserver = null;
        try {
            myserver = new SimpleServerImpl(port);
            myserver.start();
        } catch(Exception e) {
            System.err.println(e.getMessage());
            myserver.stop();
        }
    }

    /** This handler handles POST requests to the path “/compileandrun”
     *  and uses JavaRunner to compile and run Java source code.
     *  It must make sure that the content type in the client request
     *  is “text/x-java-source” and return a response code of 400 if it is not.
     */
    private class MyHandler implements HttpHandler {

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
            //STAGE1: Handles only POST requests
            if (exchange.getRequestMethod().equals("POST")) {
                if (exchange.getRequestHeaders().getFirst("Content-Type").equals("text/x-java-source")) {
                    // Run and compile given class using JavaRunner and return its output
                    try {
                        JavaRunner javaRunner = new JavaRunner();
                        InputStream is = exchange.getRequestBody();
                        String text = new String(is.readAllBytes());
                        ByteArrayInputStream in = new ByteArrayInputStream(text.getBytes());
                        String output = javaRunner.compileAndRun(in);
                        exchange.sendResponseHeaders(200, output.length());
                        OutputStream out = exchange.getResponseBody();
                        out.write(output.getBytes());
                        out.close();
                    } catch (Exception e) {
                        StringBuilder sb = new StringBuilder();
                        sb.append(e.getMessage());
                        sb.append("\n");
                        sb.append(Util.getStackTrace(e));
                        exchange.sendResponseHeaders(400, 0);
                        OutputStream out = exchange.getResponseBody();
                        out.write(sb.toString().getBytes());
                        out.close();
                    }
                } else {
                    exchange.sendResponseHeaders(400, 0);
                }
            }
            String onlyPost = "Only POST is supported\n";
            exchange.sendResponseHeaders(400, onlyPost.length());
            OutputStream out = exchange.getResponseBody();
            out.write(onlyPost.getBytes());
            out.close();
        }
    }
}
