package dev.hot.sdk.internal;

import dev.hot.sdk.HotStreamException;
import dev.hot.sdk.HotTransportException;
import dev.hot.sdk.StreamEvents;
import dev.hot.sdk.SubscribeWithEventOptions;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * subscribeWithEvent's reconnect loop: resubscribes to the same stream_id across the API's
 * 5-minute SSE rollover until the published event's final run event arrives, deduping replayed
 * run:start and terminal events by run_id. Reconnects follow {@link Backoff}: permanent errors
 * stop at once, and only consecutive unproductive connections count against maxAttempts.
 * Failures of a retried attempt are yielded but do not end the iterator; the retry run (linked by
 * origin_run_id or retry_run_id) does. A failure that announces a retry ends it only when no retry
 * run appears within the grace (see {@link RunCorrelation}). Not part of the public API.
 */
public final class ReconnectingEvents implements StreamEvents {

  private static final String NEVER = "stream ended before the published event's run completed";

  /** Bounds memory for seen run ids on busy shared streams. */
  static final int MAX_SEEN = 10_000;

  static final ScheduledThreadPoolExecutor GRACE_WATCHDOG = newWatchdog();

  private static ScheduledThreadPoolExecutor newWatchdog() {
    ScheduledThreadPoolExecutor executor =
        new ScheduledThreadPoolExecutor(
            1,
            runnable -> {
              Thread thread = new Thread(runnable, "hot-sdk-retry-grace");
              thread.setDaemon(true);
              return thread;
            });
    executor.setRemoveOnCancelPolicy(true);
    return executor;
  }

  private final Function<String, SseEvents> opener;
  private final SubscribeWithEventOptions options;
  private final Set<String> seenStart = RunWaiter.boundedSet(MAX_SEEN);
  private final Set<String> seenTerminal = RunWaiter.boundedSet(MAX_SEEN);
  private final RunCorrelation correlation;

  // volatile: close() may arrive from another thread (AsyncHotClient cancellation).
  private volatile SseEvents source;
  private volatile boolean done;
  private volatile ScheduledFuture<?> graceCloser;
  private Map<String, Object> nextEvent;
  private String streamId;
  private int failures;
  private boolean delivered;
  private boolean terminal;

  public ReconnectingEvents(
      Transport transport, Map<String, Object> body, SubscribeWithEventOptions options) {
    this(transport, body, options, RunCorrelation.GRACE_NANOS);
  }

  ReconnectingEvents(
      Transport transport,
      Map<String, Object> body,
      SubscribeWithEventOptions options,
      long graceNanos) {
    this(currentStreamId -> openStream(transport, body, options, currentStreamId), options, graceNanos);
  }

  ReconnectingEvents(
      Function<String, SseEvents> opener, SubscribeWithEventOptions options, long graceNanos) {
    this.opener = opener;
    this.options = options;
    this.correlation = new RunCorrelation(null, null, graceNanos);
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
      if (Thread.currentThread().isInterrupted()) {
        close();
        throw new HotTransportException(new InterruptedException("stream interrupted"));
      }
      Map<String, Object> event = null;
      RuntimeException failure = null;
      if (correlation.graceOver()) {
        // Before declaring the held failure final, drain any events already buffered on the
        // current connection (a retry run may already have arrived before grace elapsed).
        SseEvents current = source;
        if (current != null) {
          event = current.pollBuffered();
        }
        if (event == null) {
          close();
          return;
        }
      } else {
        try {
          if (source == null) {
            source = opener.apply(streamId);
          }
          armGrace();
          if (source.hasNext()) {
            event = source.next();
          }
        } catch (RuntimeException error) {
          if (done) {
            return;
          }
          if (Backoff.isInterrupted(error)) {
            close();
            throw error;
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
          Map<String, Object> run = StreamEvents.runOf(event);
          String runId = StreamEvents.runIdOf(event);
          boolean pendingRetry = run != null && "pending_retry".equals(run.get("status"));
          if (type.equals("run:start")) {
            if (runId != null && !seenStart.add(runId)) {
              continue;
            }
          } else {
            if (runId != null && seenTerminal.contains(runId)) {
              continue;
            }
            if (runId != null && !pendingRetry) {
              seenTerminal.add(runId);
            }
          }
          // A tied run that stops, fails for good or is cancelled ends the iterator after its
          // event (or after an earlier orphan event of its retry, replayed now).
          terminal =
              correlation.runEvent(type, run, StreamEvents.retryRunIdOf(event)).isFinal();
        }
        default -> {}
      }

      if (correlation.wakeAt() == null) {
        cancelGraceCloser();
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
      cancelGraceCloser();
      return;
    }
    SseEvents current = source;
    if (graceCloser == null && current != null) {
      graceCloser =
          GRACE_WATCHDOG.schedule(
              current::close, Math.max(0, wakeAt - System.nanoTime()), TimeUnit.NANOSECONDS);
    }
  }

  private void cancelGraceCloser() {
    ScheduledFuture<?> closer = graceCloser;
    graceCloser = null;
    if (closer != null) {
      closer.cancel(false);
    }
  }

  private static SseEvents openStream(
      Transport transport,
      Map<String, Object> body,
      SubscribeWithEventOptions options,
      String streamId) {
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
    cancelGraceCloser();
    SseEvents current = source;
    source = null;
    if (current != null) {
      current.close();
    }
  }
}
