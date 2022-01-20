package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Client {

    private final HttpClient httpClient;
    private final URI serverAddress;
    private Response response;
    private int lastVersionSent = 0;

    public Client(InetSocketAddress serverAddress, String endpoint, boolean threadPool) throws MalformedURLException {
        if (threadPool) {
            ExecutorService executorService = Executors.newFixedThreadPool(5);
            httpClient = HttpClient.newBuilder()
                    .executor(executorService)
                    .version(HttpClient.Version.HTTP_1_1)
                    .proxy(ProxySelector.of(serverAddress))
                    .build();
        } else {
            httpClient = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_1_1)
                    .proxy(ProxySelector.of(serverAddress))
                    .build();
        }
        this.serverAddress = URI.create("http://localhost:" + serverAddress.getPort() + endpoint);
    }

    public void sendConcurrentRequests(int n) {
        List<CompletableFuture<Response>> results = sendRequestsAndGetAsyncList(n);
        printAsyncResponses(results);
    }

    protected List<CompletableFuture<Response>> sendRequestsAndGetAsyncList(int n) {
        List<HttpRequest> L = initPostRequests(n);

        List<CompletableFuture<Response>> results = L.stream()
                .map(request -> httpClient.sendAsync(request,
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(response -> new Response(
                                response.statusCode(),
                                response.body())))
                .collect(Collectors.toList());
        return results;
    }

    protected void printAsyncResponses(List<CompletableFuture<Response>> results) {
        while (!results.isEmpty()) {
            for (int i = 0; i < results.size(); i++) {
                try {
                    CompletableFuture<Response> future = results.get(i);
                    if (future.isDone()) {
                        System.out.println("GOT RESPONSE: " + future.get());
                        results.remove(i);
                        break; //break out of nested for loop to restart next while loop iteration
                    }
                } catch (InterruptedException | ExecutionException e) {
                    System.out.println(e.getClass().getSimpleName() + " thrown for request " + i);
                    e.printStackTrace();
                }
            }
        }
    }

    private List<HttpRequest> initPostRequests(int n) {
        ArrayList<HttpRequest> L = new ArrayList<>(n);
        for (int i = lastVersionSent; lastVersionSent < i + n; lastVersionSent++) {
            String src = TestUtil.getVersionedValidClass(lastVersionSent);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(serverAddress)
                    .POST(HttpRequest.BodyPublishers.ofString(src))
                    .header("Content-Type", "text/x-java-source")
                    .build();
            L.add(request);
        }
        return L;
    }

    // synchronous
    public Response sendCompileAndRunRequest(String src) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(serverAddress)
                .POST(HttpRequest.BodyPublishers.ofString(src))
                .header("Content-Type", "text/x-java-source")
                .build();
        System.out.println("sending request from HTTP client to HTTP server @ " + serverAddress.toString());
        HttpResponse<String> httpResponse = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        response = new Response(httpResponse.statusCode(), httpResponse.body());
        return response;
    }

    public Response getResponse() throws IOException {
        return response;
    }

    public class Response {
        private int code;
        private String body;

        public Response(int code, String body) {
            this.code = code;
            this.body = body;
        }

        public int getCode() {
            return this.code;
        }

        public String getBody() {
            return this.body;
        }

        @Override
        public String toString() {
            return String.format("Response code: %d, Body: \"%s\"", code, body);
        }
    }
}
