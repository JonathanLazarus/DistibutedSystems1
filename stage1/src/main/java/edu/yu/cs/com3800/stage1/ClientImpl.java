package edu.yu.cs.com3800.stage1;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;


public class ClientImpl implements Client{

    private final HttpClient httpClient;
    private final String uri;
    private Response response;

    public ClientImpl(String hostName, int hostPort) throws MalformedURLException {
        httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .proxy(ProxySelector.of(new InetSocketAddress(hostName, hostPort)))
                .build();
        uri = new String("http://" + hostName + ":" + hostPort + "/compileandrun");
    }

    @Override
    public void sendCompileAndRunRequest(String src) throws IOException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(uri))
                .POST(HttpRequest.BodyPublishers.ofString(src))
                .header("Content-Type", "text/x-java-source")
                .build();
        try {
            HttpResponse<String> httpResponse = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            response = new Response(httpResponse.statusCode(), httpResponse.body());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Response getResponse() throws IOException {
        return response;
    }
}
