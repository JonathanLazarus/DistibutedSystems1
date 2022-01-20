package edu.yu.cs.com3800.stage4;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/** This handler handles POST requests to the path “/compileandrun”
 *  and uses JavaRunner to compile and run Java source code.
 *  It must make sure that the content type in the client request
 *  is “text/x-java-source” and return a response code of 400 if it is not.
 */
public class TestHandler implements HttpHandler, LoggingServer {

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
        ThreadLocal<Logger> localLogger = new ThreadLocal<>();
        if (localLogger.get() == null) localLogger.set(initializeLogging("HTTPServerHandler-port-" + 8888 + "-thread-" + Thread.currentThread().getId()));
        localLogger.get().finest("handling exchange");
        //STAGE1: Handles only POST requests

        if (exchange.getRequestMethod().equals("POST")) {
            localLogger.get().finest("is a POST exchange");
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
                    exchange.sendResponseHeaders(400, 0);
                    OutputStream out = exchange.getResponseBody();
                    String sb = e.getMessage() + "\n" + Util.getStackTrace(e);
                    out.write(sb.getBytes());
                    out.close();
                }
            } else {
                localLogger.get().finest("exchange Content-Type was not text/x-java-source\nSending 400 response");
                exchange.sendResponseHeaders(400, 0);
            }
        }
        else {
            localLogger.get().finest("was not a POST request\nSending 400 response");
            String onlyPost = "Only POST is supported\n";
            exchange.sendResponseHeaders(400, onlyPost.length());
            OutputStream out = exchange.getResponseBody();
            out.write(onlyPost.getBytes());
            out.close();
        }
    }

    private ThreadLocal<Logger> getLocalLogger(int serverPort) throws IOException {
        AtomicReference<IOException> io = new AtomicReference<>();
        ThreadLocal<Logger> localLogger = ThreadLocal.withInitial(() -> {
            try {
                return initializeLogging("GatewayHttpServer" + serverPort + "-on-thread-" + Thread.currentThread().getId());
            } catch (IOException e) {
                io.set(e);
            }
            return null;
        });
        if (localLogger.get() != null) {
            return localLogger;
        }
        else if (io.get() != null) {
            throw io.get();
        }
        else {
            throw new IOException("issue initializing a logger for server thread " + Thread.currentThread().getId());
        }
    }
}
