package dev.hot.sdk;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
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
}
