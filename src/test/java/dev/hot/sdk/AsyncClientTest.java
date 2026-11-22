package dev.hot.sdk;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.absent;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.http.HttpRequest.BodyPublishers;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AsyncClientTest {

  private WireMockServer server;

  @BeforeEach
  void start() {
    // HTTP/1.1 only, as the Hot API serves plain HTTP: the JDK 17 client
    // drops reused h2c connections mid-retry.
    server = new WireMockServer(options().dynamicPort().http2PlainDisabled(true));
    server.start();
  }

  @AfterEach
  void stop() {
    server.stop();
  }

  private AsyncHotClient async() {
    return AsyncHotClient.wrap(HotClient.builder("test-token").baseUrl(server.baseUrl()).build());
  }

  @Test
  void defaultExecutorIsADedicatedDaemonPool() throws Exception {
    server.stubFor(
        get(urlPathEqualTo("/v1/env"))
            .willReturn(aResponse().withStatus(200).withBody("{\"data\":{},\"meta\":{}}")));
    String[] thread = new String[1];
    boolean[] daemon = new boolean[1];

    async()
        .env()
        .get()
        .thenAccept(
            ignored -> {
              thread[0] = Thread.currentThread().getName();
              daemon[0] = Thread.currentThread().isDaemon();
            })
        .get(10, TimeUnit.SECONDS);

    assertTrue(thread[0].startsWith("hot-sdk-async-"), thread[0]);
    assertTrue(daemon[0]);
  }

  @Test
  void cancellingASubscriptionClosesTheStream() throws Exception {
    // One event, then a stream that dribbles keepalives for a minute.
    server.stubFor(
        get(urlPathEqualTo("/v1/runs/r1/subscribe"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "text/event-stream")
                    .withBody(
                        "data: {\"type\":\"run:update\",\"run\":{\"run_id\":\"r1\"}}\n\n"
                            + ": keepalive\n\n".repeat(600))
                    .withChunkedDribbleDelay(600, 60_000)));
    CountDownLatch first = new CountDownLatch(1);
    AtomicInteger events = new AtomicInteger();

    CompletableFuture<Void> subscription =
        async()
            .runs()
            .subscribe(
                "r1",
                event -> {
                  events.incrementAndGet();
                  first.countDown();
                });
    assertTrue(first.await(10, TimeUnit.SECONDS), "no event delivered");
    long started = System.nanoTime();
    assertTrue(subscription.cancel(true));

    // The drain loop exits once the stream is closed rather than after the full minute.
    Thread.sleep(500);
    assertTrue(subscription.isCancelled());
    assertEquals(1, events.get());
    assertTrue((System.nanoTime() - started) / 1_000_000 < 5_000);
  }

  @Test
  void streamingErrorsCompleteTheFutureExceptionally() {
    server.stubFor(
        get(urlPathEqualTo("/v1/runs/r1/subscribe"))
            .willReturn(aResponse().withStatus(404).withBody("{\"error\":{\"message\":\"nope\"}}")));

    CompletableFuture<Void> subscription = async().runs().subscribe("r1", event -> {});

    Exception error =
        org.junit.jupiter.api.Assertions.assertThrows(
            Exception.class, () -> subscription.get(10, TimeUnit.SECONDS));
    assertTrue(error.getCause() instanceof HotApiException, String.valueOf(error.getCause()));
  }

  @Test
  void asyncParityCoversStatusBlobsDownloadsSubscribePostRequestRawAndOrgEnv() throws Exception {
    byte[] blobBytes = "blob-data".getBytes(StandardCharsets.UTF_8);
    byte[] fileBytes = "file-data".getBytes(StandardCharsets.UTF_8);
    byte[] buildBytes = "build-data".getBytes(StandardCharsets.UTF_8);

    server.stubFor(
        get(urlEqualTo("/status"))
            .withHeader("Authorization", absent())
            .willReturn(aResponse().withStatus(200).withBody("{\"status\":\"ok\"}")));
    server.stubFor(
        get(urlEqualTo("/v1/blobs/b1/download"))
            .willReturn(aResponse().withStatus(200).withBody(blobBytes)));
    server.stubFor(
        get(urlEqualTo("/v1/files/f1/download"))
            .willReturn(aResponse().withStatus(200).withBody(fileBytes)));
    server.stubFor(
        get(urlEqualTo("/v1/projects/p1/builds/b1/download"))
            .willReturn(aResponse().withStatus(200).withBody(buildBytes)));
    server.stubFor(
        post(urlPathEqualTo("/v1/streams/s1/subscribe"))
            .willReturn(ClientTest.sse("{\"type\":\"stream:complete\",\"stream_id\":\"s1\"}")));
    server.stubFor(
        get(urlPathEqualTo("/v1/raw-check"))
            .willReturn(aResponse().withStatus(200).withBody("raw-ok")));
    server.stubFor(
        delete(urlPathEqualTo("/v1/orgs/o1/envs/e1/projects/p1"))
            .willReturn(aResponse().withStatus(204)));
    server.stubFor(
        put(urlPathEqualTo("/v1/orgs/o1/envs/e1/projects/p1/context/k1"))
            .willReturn(aResponse().withBody("{\"data\":{\"key\":\"k1\"},\"meta\":{}}")));
    server.stubFor(
        get(urlPathEqualTo("/v1/orgs/o1/envs/e1/projects/p1/builds/b1/download"))
            .willReturn(aResponse().withStatus(200).withBody(buildBytes)));

    AsyncHotClient a = async();
    assertEquals("ok", a.status().get(10, TimeUnit.SECONDS).get("status"));

    try (InputStream body = a.blobs().download("b1").get(10, TimeUnit.SECONDS).body()) {
      assertArrayEquals(blobBytes, body.readAllBytes());
    }
    try (InputStream body = a.files().download("f1").get(10, TimeUnit.SECONDS).body()) {
      assertArrayEquals(fileBytes, body.readAllBytes());
    }
    try (InputStream body = a.builds().download("p1", "b1").get(10, TimeUnit.SECONDS).body()) {
      assertArrayEquals(buildBytes, body.readAllBytes());
    }
    List<String> postTypes = new ArrayList<>();
    a.streams()
        .subscribePost("s1", null, event -> postTypes.add(StreamEvents.typeOf(event)))
        .get(10, TimeUnit.SECONDS);
    assertEquals(List.of("stream:complete"), postTypes);

    try (InputStream body =
        a.requestRaw("GET", "/raw-check", null, null, BodyPublishers.noBody())
            .get(10, TimeUnit.SECONDS)
            .body()) {
      assertEquals("raw-ok", new String(body.readAllBytes(), StandardCharsets.UTF_8));
    }

    AsyncHotClient.OrgEnv orgEnv = a.orgs().env("o1", "e1");
    orgEnv.projects().delete("p1").get(10, TimeUnit.SECONDS);
    assertEquals(
        "k1",
        orgEnv.context().update("p1", "k1", Map.of("value", "v")).get(10, TimeUnit.SECONDS).get("key"));
    try (InputStream body = orgEnv.downloadBuild("p1", "b1").get(10, TimeUnit.SECONDS).body()) {
      assertArrayEquals(buildBytes, body.readAllBytes());
    }
  }

  @Test
  void cancellingSubscriptionInterruptsBlockedConnectBeforeHeaders() throws Exception {
    CountDownLatch accepted = new CountDownLatch(1);
    CountDownLatch workerDone = new CountDownLatch(1);
    try (ServerSocket listener = new ServerSocket(0, 5, InetAddress.getLoopbackAddress())) {
      List<Socket> sockets = new ArrayList<>();
      Thread acceptor =
          new Thread(
              () -> {
                try {
                  Socket s = listener.accept();
                  sockets.add(s);
                  accepted.countDown();
                } catch (Exception ignored) {
                }
              });
      acceptor.setDaemon(true);
      acceptor.start();

      HotClient hanging =
          HotClient.builder("test-token")
              .baseUrl("http://127.0.0.1:" + listener.getLocalPort())
              .build();
      AsyncHotClient async =
          AsyncHotClient.wrap(
              hanging,
              task -> {
                Thread t =
                    new Thread(
                        () -> {
                          try {
                            task.run();
                          } finally {
                            workerDone.countDown();
                          }
                        });
                t.setDaemon(true);
                t.start();
              });

      CompletableFuture<Void> sub = async.runs().subscribe("r1", event -> {});
      assertTrue(accepted.await(5, TimeUnit.SECONDS), "server did not accept connection");

      assertTrue(sub.cancel(true));
      assertTrue(
          workerDone.await(5, TimeUnit.SECONDS),
          "worker thread should exit promptly after cancel(true) interrupts httpClient.send()");

      for (Socket s : sockets) {
        s.close();
      }
    }
  }
}
