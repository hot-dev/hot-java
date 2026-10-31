package dev.hot.sdk.internal;

import dev.hot.sdk.HotStreamException;
import dev.hot.sdk.StreamEvents;
import dev.hot.sdk.SubscribeWithEventOptions;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * subscribeWithEvent's reconnect loop: resubscribes to the same stream_id across the API's
 * 5-minute SSE rollover until the published event's final run event arrives, deduping replayed
 * run:start and terminal events by run_id. Reconnects follow {@link Backoff}: permanent errors
 * stop at once, and only consecutive unproductive connections count against maxAttempts.
 * Failures of a retried attempt are yielded but do not end the iterator; the retry run (linked by
 * origin_run_id) does. A failure that announces a retry ends it only when no retry run appears
 * within the grace (see {@link RunCorrelation}). Not part of the public API.
 */
public final class ReconnectingEvents implements StreamEvents {

  private static final String NEVER = "stream ended before the published event's run completed";

  private final Transport transport;
  private final Map<String, Object> body;
  private final SubscribeWithEventOptions options;
  private final Set<String> seenStart = new HashSet<>();
  private final Set<String> seenTerminal = new HashSet<>();
  private final RunCorrelation correlation = new RunCorrelation(null, null);

  private static final ScheduledExecutorService GRACE_WATCHDOG =
      Executors.newSingleThreadScheduledExecutor(
          runnable -> {
            Thread thread = new Thread(runnable, "hot-sdk-retry-grace");
            thread.setDaemon(true);
            return thread;
          });

  // volatile: close() may arrive from another thread (AsyncHotClient cancellation).
  private volatile SseEvents source;
  private volatile boolean done;
  private Map<String, Object> nextEvent;
  private String streamId;
  private int failures;
  private boolean delivered;
  private boolean terminal;
  private ScheduledFuture<?> graceCloser;

  public ReconnectingEvents(
      Transport transport, Map<String, Object> body, SubscribeWithEventOptions options) {
    this.transport = transport;
    this.body = body;
    this.options = options;
  }

  @Override
  public boolean hasNext() {
    if (nextEvent != null) {
      return true;
    }
    if (done) {
      return false;
    }
    advance();
    return nextEvent != null;
  }

  @Override
  public Map<String, Object> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Map<String, Object> event = nextEvent;
    nextEvent = null;
    if (terminal) {
      close();
    }
    return event;
  }

  @Override
  public void close() {
    done = true;
    closeSource();
  }

  private void advance() {
    while (!done) {
      if (correlation.graceOver()) {
        // A held failure's retry run never appeared: that failure (already yielded) is final.
        close();
        return;
      }
      Map<String, Object> event = null;
      RuntimeException failure = null;
      try {
        if (source == null) {
          source = open();
        }
        armGrace();
        if (source.hasNext()) {
          event = source.next();
        }
      } catch (RuntimeException error) {
        if (done) {
          return;
        }
        if (correlation.graceOver()) {
          continue;
        }
        if (Backoff.isPermanent(error) || streamId == null) {
          close();
          throw error;
        }
        failure = error;
      }
      if (event == null) {
        if (correlation.graceOver()) {
          continue;
        }
        onConnectionEnd(failure);
        continue;
      }

      String type = StreamEvents.typeOf(event);
      switch (type) {
        case "event:published" -> {
          if (streamId != null) {
            continue;
          }
          if (event.get("stream_id") instanceof String published) {
            streamId = published;
          }
          if (event.get("event_id") instanceof String published) {
            correlation.eventId(published);
          }
        }
        case "run:start", "run:stop", "run:fail", "run:cancel" -> {
          String runId = StreamEvents.runIdOf(event);
          Set<String> seen = type.equals("run:start") ? seenStart : seenTerminal;
          if (runId != null && !seen.add(runId)) {
            continue;
          }
          // A tied run that stops, fails for good or is cancelled ends the iterator after its
          // event (or after an earlier orphan event of its retry, replayed now).
          terminal = correlation.runEvent(type, StreamEvents.runOf(event)).isFinal();
        }
        default -> {}
      }

      delivered = true;
      nextEvent = event;
      return;
    }
  }

  /** While a failure waits for its retry run, closes the source when the grace ends. */
  private void armGrace() {
    Long wakeAt = correlation.wakeAt();
    if (wakeAt == null) {
      if (graceCloser != null) {
        graceCloser.cancel(false);
        graceCloser = null;
      }
      return;
    }
    if (graceCloser == null && source != null) {
      SseEvents current = source;
      graceCloser =
          GRACE_WATCHDOG.schedule(
              current::close, Math.max(0, wakeAt - System.nanoTime()), TimeUnit.NANOSECONDS);
    }
  }

  private SseEvents open() {
    Map<String, String> query = new HashMap<>();
    if (options.project() != null) {
      query.put("project", options.project());
    }
    // Without a deadline, the idle timeout also bounds the wait for response headers.
    if (streamId == null) {
      return transport.stream(
          "POST",
          "/streams/subscribe-with-event",
          body,
          query,
          RunWaiter.IDLE_TIMEOUT,
          RunWaiter.IDLE_TIMEOUT);
    }
    return transport.stream(
        "GET",
        "/streams/" + Transport.enc(streamId) + "/subscribe",
        null,
        query,
        RunWaiter.IDLE_TIMEOUT,
        RunWaiter.IDLE_TIMEOUT);
  }

  /** Decides what follows a connection that ended cleanly ({@code error} null) or failed. */
  private void onConnectionEnd(RuntimeException error) {
    boolean rolledOver = source != null && source.sawComplete();
    closeSource();
    if (terminal) {
      done = true;
      return;
    }
    if (streamId == null) {
      done = true;
      if (error != null) {
        throw error;
      }
      throw new HotStreamException("stream ended before event:published was received");
    }
    boolean productive = delivered || rolledOver;
    delivered = false;
    if (productive) {
      failures = 0;
      if (error == null) {
        return;
      }
    }
    failures++;
    if (failures > options.maxAttempts()) {
      done = true;
      if (error != null) {
        throw error;
      }
      throw new HotStreamException(NEVER);
    }
    try {
      Backoff.sleep(Backoff.reconnectDelayMillis(failures, error), Long.MAX_VALUE, NEVER, error);
    } catch (RuntimeException interrupted) {
      done = true;
      throw interrupted;
    }
  }

  private void closeSource() {
    if (graceCloser != null) {
      graceCloser.cancel(false);
      graceCloser = null;
    }
    SseEvents current = source;
    source = null;
    if (current != null) {
      current.close();
    }
  }
}
