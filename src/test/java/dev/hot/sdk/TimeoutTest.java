package dev.hot.sdk;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/** Waiter timeouts cover the wait for response headers: a server that never replies times out. */
class TimeoutTest {

  private ServerSocket listener;
  private final List<Socket> accepted = new CopyOnWriteArrayList<>();

  @BeforeEach
  void start() throws IOException {
    listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
    Thread acceptor =
        new Thread(
            () -> {
              while (!listener.isClosed()) {
                try {
                  accepted.add(listener.accept()); // accept, read nothing, never reply
                } catch (IOException closed) {
                  return;
                }
              }
            });
    acceptor.setDaemon(true);
    acceptor.start();
  }

  @AfterEach
  void stop() throws IOException {
    listener.close();
    for (Socket socket : accepted) {
      socket.close();
    }
  }

  private HotClient client() {
    return HotClient.builder("test-token")
        .baseUrl("http://127.0.0.1:" + listener.getLocalPort())
        .build();
  }

  private static void timesOutWithin(Executable call, long limitMillis) {
    long started = System.nanoTime();
    assertThrows(HotTimeoutException.class, call);
    long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
    assertTrue(elapsedMillis < limitMillis, "timed out after " + elapsedMillis + " ms");
  }

  @Test
  void runWaitTimesOutBeforeHeaders() {
    timesOutWithin(
        () -> client().runs().waitFor("r1", new RunWaitOptions().timeout(Duration.ofSeconds(2))),
        5_000);
  }

  @Test
  void taskWaitTimesOutBeforeHeaders() {
    timesOutWithin(
        () -> client().tasks().waitFor("t1", new TaskWaitOptions().timeout(Duration.ofSeconds(2))),
        5_000);
  }

  @Test
  void runResultWaitTimesOutBeforeHeaders() {
    timesOutWithin(
        () ->
            client()
                .streams()
                .waitForRunResult("s1", "e1", new WaitOptions().timeout(Duration.ofSeconds(2))),
        5_000);
  }

  @Test
  void callHotTimesOutBeforeHeaders() {
    timesOutWithin(
        () ->
            client()
                .events()
                .callHot("demo/fn", List.of(), new CallOptions().timeout(Duration.ofSeconds(2))),
        5_000);
  }
}
