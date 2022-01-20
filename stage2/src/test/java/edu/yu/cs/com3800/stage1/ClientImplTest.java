package edu.yu.cs.com3800.stage1;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class ClientImplTest {
    private final int port = 9000;
    private SimpleServerImpl server;
    private ClientImpl client;
    private final String src = "public class HelloWorld {\n" +
            "    private String hello;\n" +
            "\n" +
            "    public HelloWorld() {\n" +
            "        hello = \"Hello World!\";\n" +
            "    }\n" +
            "    \n" +
            "    public String run() {\n" +
            "        return hello;\n" +
            "    }\n" +
            "}";

    private final String invalidSrc = "public class HelloWorld {\n" +
            "    private String hello;\n" +
            "\n" +
            "    public HelloWorld() {\n" +
            "        hello = \"Hello World!\";\n" +
            "    }\n" +
            "    \n" +
            "    public int run() {\n" +
            "        return 301;\n" +
            "    }\n" +
            "}";

    @Before
    public void setUp() throws Exception {
        server = new SimpleServerImpl(port);
        server.start();
        client = new ClientImpl("localhost", port);
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }

    @Test
    public void sendCompileAndRunRequest() {
        try {
            client.sendCompileAndRunRequest(src);
            Client.Response response = client.getResponse();
            System.out.println("Expected response:");
            System.out.println("200\nHello World!");
            System.out.println("Actual response:");
            System.out.println(response.getCode());
            System.out.println(response.getBody());
            assertEquals(200, response.getCode());
            assertEquals("Hello World!", response.getBody());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void get400Response() {
        try {
            client.sendCompileAndRunRequest(invalidSrc);
            Client.Response response = client.getResponse();
            System.out.println("Expected response:");
            System.out.println("400");
            System.out.println("Actual response:");
            System.out.println(response.getCode());
            System.out.println(response.getBody());
            assertEquals(400, response.getCode());
            assertNotEquals(301, response.getBody());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}