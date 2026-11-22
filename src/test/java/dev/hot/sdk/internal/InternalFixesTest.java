package dev.hot.sdk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.hot.sdk.HotStreamException;
import dev.hot.sdk.HotTransportException;
import dev.hot.sdk.StreamEvents;
import dev.hot.sdk.SubscribeWithEventOptions;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Modifier;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Unit tests for internal transport, SSE parsing, correlation, watchdog, and encoding fixes. */
class InternalFixesTest {

  private static SseEvents sseFromString(String text) {
    return sseFromBytes(text.getBytes(StandardCharsets.UTF_8), null, SseEvents.MAX_EVENT_BYTES);
  }

  private static SseEvents sseFromBytes(byte[] bytes, Duration idleTimeout, int maxEventBytes) {
    return new SseEvents(new ByteArrayInputStream(bytes), false, idleTimeout, maxEventBytes);
  }

  // --- Path validation ---

  @Test
  void encRejectsNullEmptyAndDotSegments() {
    assertThrows(IllegalArgumentException.class, () -> Transport.enc(null));
    assertThrows(IllegalArgumentException.class, () -> Transport.enc(""));
    assertThrows(IllegalArgumentException.class, () -> Transport.enc("."));
    assertThrows(IllegalArgumentException.class, () -> Transport.enc(".."));
    assertEquals("a%2Fb%20c", Transport.enc("a/b c"));
    assertEquals(".hidden", Transport.enc(".hidden"));
  }

  @Test
  void encPathStripsLeadingSlashesAndRejectsTraversalOrEmptySegments() {
    assertThrows(IllegalArgumentException.class, () -> Transport.encPath(null));
    assertThrows(IllegalArgumentException.class, () -> Transport.encPath(""));
    assertThrows(IllegalArgumentException.class, () -> Transport.encPath("/"));
    assertThrows(IllegalArgumentException.class, () -> Transport.encPath("///"));
    assertThrows(IllegalArgumentException.class, () -> Transport.encPath("../x"));
    assertThrows(IllegalArgumentException.class, () -> Transport.encPath("/../x"));
    assertThrows(IllegalArgumentException.class, () -> Transport.encPath("a/./b"));
    assertThrows(IllegalArgumentException.class, () -> Transport.encPath("a/../b"));
    assertThrows(IllegalArgumentException.class, () -> Transport.encPath("a//b"));
    assertThrows(IllegalArgumentException.class, () -> Transport.encPath("a/b/"));

    assertEquals("a/b%20c/file.txt", Transport.encPath("/a/b c/file.txt"));
    assertEquals("a/b", Transport.encPath("///a/b"));
  }

  // --- Transport lifecycle, normalization & bounds ---

  @Test
  void defaultClientsShareSingleLazyHttpClient() {
    Transport t1 = new Transport("tok-1", "http://127.0.0.1:1", null, null);
    Transport t2 = new Transport("tok-2", "http://127.0.0.1:2", null, null);
    assertSame(t1.httpClient(), t2.httpClient());
    assertEquals(HttpClient.Redirect.NEVER, t1.httpClient().followRedirects());
  }

  @Test
  void transportRejectsRedirectingHttpClientAndNonPositiveTimeout() {
    HttpClient redirecting =
        HttpClient.newBuilder().followRedirects(HttpClient.Redirect.ALWAYS).build();
    assertThrows(
        IllegalArgumentException.class,
        () -> new Transport("tok", "https://api.hot.dev", null, redirecting));
    assertThrows(
        IllegalArgumentException.class,
        () -> new Transport("tok", "https://api.hot.dev", Duration.ZERO, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> new Transport("tok", "https://api.hot.dev", Duration.ofSeconds(-5), null));
  }

  @Test
  void transportNormalizesBaseUrlTrailingSlashesAndV1AndNull() {
    assertEquals(
        Transport.DEFAULT_BASE_URL, new Transport("tok", null, null, null).baseUrl());
    assertEquals(
        "https://api.hot.dev",
        new Transport("tok", "https://api.hot.dev/v1/", null, null).baseUrl());
    assertEquals(
        "https://api.hot.dev",
        new Transport("tok", "https://api.hot.dev/v1", null, null).baseUrl());
    assertEquals(
        "http://localhost:4681",
        new Transport("tok", "http://localhost:4681///v1///", null, null).baseUrl());
  }

  @Test
  void retry429DelayAddsJitterAndCapsAt30Seconds() {
    Set<Long> samples = new HashSet<>();
    for (int i = 0; i < 64; i++) {
      long delay = Transport.retry429DelayMillis(1);
      assertTrue(delay >= 1_000 && delay <= 1_250, "delay=" + delay);
      samples.add(delay);
    }
    assertTrue(samples.size() > 1, "expected jittered delays, got " + samples);
    assertEquals(30_000L, Transport.retry429DelayMillis(30));
    assertEquals(30_000L, Transport.retry429DelayMillis(120));
  }

  @Test
  void readBoundedUtf8CapsReadsAtLimit() {
    byte[] large = "x".repeat(2048).getBytes(StandardCharsets.UTF_8);
    String read = Transport.readBoundedUtf8(new ByteArrayInputStream(large), 128);
    assertEquals(128, read.length());
  }

  // --- SSE parser fixes ---

  @Test
  void sseHandlesCrLfFraming() {
    String body =
        "data: {\"type\":\"stream:data\",\"payload\":\"a\"}\r\n\r\n"
            + "data: {\"type\":\"run:stop\",\"run\":{\"result\":\"ok\"}}\r\n\r\n";
    List<Map<String, Object>> events = new ArrayList<>();
    try (SseEvents sse = sseFromString(body)) {
      sse.forEachRemaining(events::add);
    }
    assertEquals(2, events.size());
    assertEquals("stream:data", StreamEvents.typeOf(events.get(0)));
    assertEquals("run:stop", StreamEvents.typeOf(events.get(1)));
  }

  @Test
  void sseJoinsMultiLineDataWithNewlinePerWhatwgSpec() {
    // 1. Valid multi-line JSON parses across data: lines.
    // 2. Tokens split across data: lines (1\n2 and "a\nb") MUST NOT concatenate as 12 / "ab".
    String body =
        "data: {\"type\":\"stream:data\",\"n\":1\n"
            + "data: 2}\n\n"
            + "data: {\"type\":\"stream:data\",\"text\":\"a\n"
            + "data: b\"}\n\n"
            + "data: {\n"
            + "data: \"type\": \"run:stop\",\n"
            + "data: \"run\": {\"result\": \"ok\"}\n"
            + "data: }\n\n";
    List<Map<String, Object>> events = new ArrayList<>();
    try (SseEvents sse = sseFromString(body)) {
      sse.forEachRemaining(events::add);
    }
    assertEquals(1, events.size());
    assertEquals("run:stop", StreamEvents.typeOf(events.get(0)));
    assertEquals("ok", StreamEvents.runOf(events.get(0)).get("result"));
  }

  @Test
  void sseStripsSingleLeadingUtf8BomAndDecodesUtf8() {
    byte[] bom = new byte[] {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};
    byte[] rest =
        "data: {\"type\":\"stream:data\",\"text\":\"caf\u00E9 \u2603\"}\n\n"
            .getBytes(StandardCharsets.UTF_8);
    byte[] combined = new byte[bom.length + rest.length];
    System.arraycopy(bom, 0, combined, 0, bom.length);
    System.arraycopy(rest, 0, combined, bom.length, rest.length);

    List<Map<String, Object>> events = new ArrayList<>();
    try (SseEvents sse = sseFromBytes(combined, null, SseEvents.MAX_EVENT_BYTES)) {
      sse.forEachRemaining(events::add);
    }
    assertEquals(1, events.size());
    assertEquals("caf\u00E9 \u2603", events.get(0).get("text"));
  }

  @Test
  void sseCapsMaxEventDataSizeAndAllowsMultipleSmallerEvents() {
    assertEquals(16 * 1024 * 1024, SseEvents.MAX_EVENT_BYTES);

    // Three 64-byte events pass a 128-byte per-event cap (proving buffer resets per event).
    String single = "data: {\"type\":\"stream:data\",\"pad\":\"" + "a".repeat(20) + "\"}\n\n";
    List<Map<String, Object>> ok = new ArrayList<>();
    try (SseEvents sse = sseFromBytes((single + single + single).getBytes(StandardCharsets.UTF_8), null, 128)) {
      sse.forEachRemaining(ok::add);
    }
    assertEquals(3, ok.size());

    // Single event exceeding cap throws HotStreamException.
    String oversized = "data: {\"type\":\"stream:data\",\"pad\":\"" + "a".repeat(200) + "\"}\n\n";
    try (SseEvents sse = sseFromBytes(oversized.getBytes(StandardCharsets.UTF_8), null, 128)) {
      assertThrows(HotStreamException.class, sse::hasNext);
    }
  }

  /** InputStream that yields initial bytes, then blocks until closed (like a live HTTP socket). */
  private static final class BlockingUntilClosedInputStream extends InputStream {
    private final byte[] initial;
    private int pos;
    private volatile boolean closed;

    BlockingUntilClosedInputStream(String initialText) {
      this.initial = initialText.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public int read() throws IOException {
      byte[] one = new byte[1];
      int n = read(one, 0, 1);
      return n == -1 ? -1 : (one[0] & 0xff);
    }

    @Override
    public synchronized int read(byte[] b, int off, int len) throws IOException {
      if (pos < initial.length) {
        int count = Math.min(len, initial.length - pos);
        System.arraycopy(initial, pos, b, off, count);
        pos += count;
        return count;
      }
      while (!closed) {
        try {
          wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("interrupted", e);
        }
      }
      throw new IOException("stream closed");
    }

    @Override
    public synchronized void close() {
      closed = true;
      notifyAll();
    }
  }

  @Test
  void sseCloseMidBlockDoesNotEmitPartialEvent() throws Exception {
    // Stream emits an unterminated data line and then closes when read blocks for the next line.
    BlockingUntilClosedInputStream in =
        new BlockingUntilClosedInputStream(
            "data: {\"type\":\"run:stop\",\"run\":{\"result\":\"partial\"}}\n");
    SseEvents sse = new SseEvents(in, false, null);

    Thread closer =
        new Thread(
            () -> {
              try {
                Thread.sleep(60);
              } catch (InterruptedException ignored) {
                return;
              }
              sse.close();
            });
    closer.setDaemon(true);
    closer.start();

    // Closing mid-block either throws HotTransportException or returns false, NEVER emits partial.
    try {
      assertFalse(sse.hasNext(), "partial block must not be emitted after close()");
    } catch (HotTransportException expected) {
      // Expected when close() unblocks readLine() with IOException.
    }
  }

  @Test
  void sseIdleTimeoutMidBlockDoesNotEmitPartialEventAndFormatsSubSecondMs() throws Exception {
    // Write a valid JSON data line WITHOUT its terminating blank line, then go silent.
    BlockingUntilClosedInputStream in =
        new BlockingUntilClosedInputStream(
            "data: {\"type\":\"run:stop\",\"run\":{\"result\":\"partial\"}}\n");

    try (SseEvents sse = new SseEvents(in, false, Duration.ofMillis(120))) {
      HotTransportException error = assertThrows(HotTransportException.class, sse::hasNext);
      assertTrue(error.getMessage().contains("120 ms"), error.getMessage());
    }
  }

  // --- Watchdog ScheduledThreadPoolExecutor remove-on-cancel ---

  @Test
  void watchdogsEnableRemoveOnCancelPolicy() {
    assertTrue(SseEvents.IDLE_WATCHDOG.getRemoveOnCancelPolicy());
    assertTrue(RunWaiter.WATCHDOG.getRemoveOnCancelPolicy());
    assertTrue(ReconnectingEvents.GRACE_WATCHDOG.getRemoveOnCancelPolicy());

    // Scheduling and immediately cancelling an idle watchdog leaves it evicted from the queue.
    SseEvents sse =
        new SseEvents(
            new ByteArrayInputStream(new byte[0]), false, Duration.ofHours(1));
    sse.close();
    assertEquals(0, SseEvents.IDLE_WATCHDOG.getQueue().size());
  }

  // --- RunWaiter wraparound & bounded set ---

  @Test
  void runWaiterExpiredHandlesNanoTimeWraparound() {
    // Deadline just before Long.MAX_VALUE; now wrapped past Long.MIN_VALUE -> expired!
    long deadline = Long.MAX_VALUE - 5;
    long wrappedNow = Long.MIN_VALUE + 5;
    assertTrue(RunWaiter.expired(wrappedNow, deadline));
    assertFalse(RunWaiter.expired(Long.MAX_VALUE - 10, deadline));

    // Deadline wrapped past Long.MIN_VALUE; now still near Long.MAX_VALUE -> NOT expired yet!
    long wrappedDeadline = Long.MIN_VALUE + 5;
    assertFalse(RunWaiter.expired(Long.MAX_VALUE - 5, wrappedDeadline));
    assertTrue(RunWaiter.expired(Long.MIN_VALUE + 5, wrappedDeadline));
  }

  @Test
  void boundedSetCapsSizeWithFifoEviction() {
    Set<String> set = RunWaiter.boundedSet(3);
    assertTrue(set.add("a"));
    assertTrue(set.add("b"));
    assertTrue(set.add("c"));
    assertTrue(set.add("d"));
    assertEquals(3, set.size());
    assertFalse(set.contains("a"));
    assertTrue(set.contains("d"));
  }

  // --- RunCorrelation retry_run_id & orphan cap ---

  @Test
  void runCorrelationCorrelatesRetryRunIdImmediatelyAndFlushesChunks() {
    List<String> chunks = new ArrayList<>();
    RunCorrelation correlation = new RunCorrelation("e1", chunks::add);

    // Chunk for retry run rB arrives before run:fail names retry_run_id.
    correlation.chunk(
        Map.of("type", "stream:data", "run_id", "rB", "payload", Map.of("text", "early-")));
    assertEquals(List.of(), chunks);

    // run:fail on rA carries retry_run_id: rB -> correlates rB and flushes early chunks at once.
    Map<String, Object> rAFail =
        Map.of(
            "run_id", "rA",
            "event_id", "e1",
            "run_type", "event",
            "status", "pending_retry",
            "retry_attempt", 1);
    assertFalse(correlation.runEvent("run:fail", rAFail, "rB").isFinal());
    assertEquals(List.of("early-"), chunks);

    // Subsequent chunk for rB streams immediately without waiting for run:start rB.
    correlation.chunk(
        Map.of("type", "stream:data", "run_id", "rB", "payload", Map.of("text", "live")));
    assertEquals(List.of("early-", "live"), chunks);

    // rB finishes even if origin_run_id is omitted (correlated via retry_run_id).
    Map<String, Object> rBStop =
        Map.of("run_id", "rB", "event_id", "e9", "status", "succeeded", "result", "ok");
    RunCorrelation.Outcome outcome = correlation.runEvent("run:stop", rBStop, null);
    assertEquals(RunCorrelation.Kind.STOP, outcome.kind());
    assertEquals("rB", outcome.run().get("run_id"));
  }

  @Test
  void runCorrelationCapsOrphansAndRetainsRecentOnes() {
    RunCorrelation correlation = new RunCorrelation("e1", null);
    for (int i = 0; i < RunCorrelation.MAX_ORPHANS + 500; i++) {
      Map<String, Object> unrelated =
          Map.of("run_id", "u" + i, "origin_run_id", "orig" + i, "status", "succeeded");
      correlation.runEvent("run:stop", unrelated, null);
    }
    assertEquals(RunCorrelation.MAX_ORPHANS, correlation.orphanCount());

    // A newly arriving orphan for our run is retained and replayed when our origin arrives.
    Map<String, Object> retry =
        Map.of(
            "run_id", "rB",
            "event_id", "e9",
            "origin_run_id", "rA",
            "status", "succeeded",
            "result", "recovered");
    assertFalse(correlation.runEvent("run:stop", retry, null).isFinal());

    Map<String, Object> origin =
        Map.of("run_id", "rA", "event_id", "e1", "status", "failed", "retry_attempt", 1);
    RunCorrelation.Outcome outcome = correlation.runEvent("run:fail", origin, null);
    assertEquals(RunCorrelation.Kind.STOP, outcome.kind());
    assertEquals("rB", outcome.run().get("run_id"));
  }

  // --- ReconnectingEvents graceOver drain & pending_retry seenTerminal ---

  @Test
  void reconnectingEventsGraceOverDrainsBufferedRetryEvent() throws Exception {
    String sse =
        "data: {\"type\":\"event:published\",\"stream_id\":\"s1\",\"event_id\":\"e1\"}\n\n"
            + "data: {\"type\":\"run:fail\",\"run\":{\"run_id\":\"rA\",\"event_id\":\"e1\","
            + "\"status\":\"failed\",\"retry_attempt\":1}}\n\n"
            + "data: {\"type\":\"run:stop\",\"run\":{\"run_id\":\"rB\",\"event_id\":\"e9\","
            + "\"origin_run_id\":\"rA\",\"status\":\"succeeded\",\"result\":\"recovered\"}}\n\n";
    AtomicInteger opens = new AtomicInteger();
    ReconnectingEvents events =
        new ReconnectingEvents(
            streamId -> {
              opens.incrementAndGet();
              return sseFromString(sse);
            },
            new SubscribeWithEventOptions(),
            TimeUnit.MILLISECONDS.toNanos(40));

    try (events) {
      assertTrue(events.hasNext());
      assertEquals("event:published", StreamEvents.typeOf(events.next()));
      assertTrue(events.hasNext());
      assertEquals("run:fail", StreamEvents.typeOf(events.next()));

      // Wait past the 40ms grace window BEFORE reading the already-buffered retry run:stop.
      Thread.sleep(90);

      assertTrue(events.hasNext(), "buffered retry event must be drained even after graceOver()");
      Map<String, Object> stop = events.next();
      assertEquals("run:stop", StreamEvents.typeOf(stop));
      assertEquals("rB", StreamEvents.runIdOf(stop));
      assertFalse(events.hasNext());
    }
    assertEquals(1, opens.get());
  }

  @Test
  void reconnectingEventsPendingRetryDoesNotSuppressLaterRunFail() {
    String sse =
        "data: {\"type\":\"event:published\",\"stream_id\":\"s1\",\"event_id\":\"e1\"}\n\n"
            + "data: {\"type\":\"run:fail\",\"run\":{\"run_id\":\"rA\",\"event_id\":\"e1\","
            + "\"status\":\"pending_retry\",\"retry_attempt\":1}}\n\n"
            + "data: {\"type\":\"run:fail\",\"retry_run_id\":\"rB\",\"run\":{\"run_id\":\"rA\","
            + "\"event_id\":\"e1\",\"status\":\"pending_retry\",\"retry_attempt\":1}}\n\n"
            + "data: {\"type\":\"run:stop\",\"run\":{\"run_id\":\"rB\",\"event_id\":\"e9\","
            + "\"status\":\"succeeded\",\"result\":\"ok\"}}\n\n";
    List<Map<String, Object>> seen = new ArrayList<>();
    try (ReconnectingEvents events =
        new ReconnectingEvents(
            streamId -> sseFromString(sse),
            new SubscribeWithEventOptions(),
            RunCorrelation.GRACE_NANOS)) {
      events.forEachRemaining(seen::add);
    }

    assertEquals(4, seen.size());
    assertEquals("event:published", StreamEvents.typeOf(seen.get(0)));
    assertNull(StreamEvents.retryRunIdOf(seen.get(1)));
    assertEquals("rB", StreamEvents.retryRunIdOf(seen.get(2)));
    assertEquals("run:stop", StreamEvents.typeOf(seen.get(3)));
    assertEquals("rB", StreamEvents.runIdOf(seen.get(3)));
  }

  @Test
  void reconnectingEventsStopsImmediatelyWhenInterrupted() {
    AtomicInteger opens = new AtomicInteger();
    ReconnectingEvents events =
        new ReconnectingEvents(
            streamId -> {
              opens.incrementAndGet();
              throw new HotTransportException(new InterruptedException("cancelled"));
            },
            new SubscribeWithEventOptions(),
            RunCorrelation.GRACE_NANOS);
    try (events) {
      assertThrows(HotTransportException.class, events::hasNext);
    }
    assertEquals(1, opens.get());
  }

  // --- Json & Multipart ---

  @Test
  void jsonEnvelopeHandlesBlankBodiesAndMapperIsPackagePrivate() throws Exception {
    Map<String, Object> blank = Json.envelope("   \r\n\t ");
    assertNull(blank.get("data"));
    assertEquals(Map.of(), blank.get("meta"));
    assertNull(Json.tryReadObject("   "));

    int modifiers = Json.class.getDeclaredField("MAPPER").getModifiers();
    assertFalse(Modifier.isPublic(modifiers), "Json.MAPPER should be package-private");
  }

  @Test
  void multipartStripsCrLfFromFieldNamesAndFilenames() {
    Map<String, String> fields = new LinkedHashMap<>();
    fields.put("hash\r\nX-Injected: yes", "abc123");
    Multipart.Encoded encoded =
        Multipart.encode(
            fields,
            "file\r\nEvil: 1",
            "build\r\nX-Header: bad\"name.tar.gz",
            "bytes".getBytes(StandardCharsets.UTF_8));
    String body = new String(encoded.body(), StandardCharsets.UTF_8);

    assertTrue(body.contains("name=\"hashX-Injected: yes\""));
    assertTrue(body.contains("name=\"fileEvil: 1\"; filename=\"buildX-Header: bad%22name.tar.gz\""));
    assertFalse(body.contains("\r\nX-Injected:"));
    assertFalse(body.contains("\r\nX-Header:"));
  }

  // Avoid unused import warning for IOException / InputStream if needed.
  @Test
  void readBoundedUtf8HandlesIoExceptionGracefully() {
    InputStream failing =
        new InputStream() {
          @Override
          public int read() throws IOException {
            throw new IOException("boom");
          }
        };
    assertEquals("", Transport.readBoundedUtf8(failing, 64));
  }
}
