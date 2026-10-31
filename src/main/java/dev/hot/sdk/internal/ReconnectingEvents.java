package dev.hot.sdk.internal;

import dev.hot.sdk.HotStreamException;
import dev.hot.sdk.StreamEvents;
import dev.hot.sdk.SubscribeWithEventOptions;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * subscribeWithEvent's reconnect loop: resubscribes to the same stream_id across the API's
 * 5-minute SSE rollover until the published event's final run event arrives, deduping replayed
 * run:start and terminal events by run_id. Reconnects follow {@link Backoff}: permanent errors
 * stop at once, and only consecutive unproductive connections count against maxAttempts. A
 * pending_retry run:fail is passed through but does not end the iterator; the retry run (linked
 * by origin_run_id) does. Not part of the public API.
 */
public final class ReconnectingEvents implements StreamEvents {

  private static final String NEVER = "stream ended before the published event's run completed";

  private final Transport transport;
  private final Map<String, Object> body;
  private final SubscribeWithEventOptions options;
  private final Set<String> seenStart = new HashSet<>();
  private final Set<String> seenTerminal = new HashSet<>();
  private final RunCorrelation correlation = new RunCorrelation(null);

  // volatile: close() may arrive from another thread (AsyncHotClient cancellation).
  private volatile SseEvents source;
  private volatile boolean done;
  private Map<String, Object> nextEvent;
  private String streamId;
  private int failures;
  private boolean delivered;
  private boolean terminal;

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
      Map<String, Object> event = null;
      RuntimeException failure = null;
      try {
        if (source == null) {
          source = open();
        }
        if (source.hasNext()) {
          event = source.next();
        }
      } catch (RuntimeException error) {
        if (done) {
          return;
        }
        if (Backoff.isPermanent(error) || streamId == null) {
          close();
          throw error;
        }
        failure = error;
      }
      if (event == null) {
        onConnectionEnd(failure);
        continue;
      }

      switch (StreamEvents.typeOf(event)) {
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
        case "run:start" -> {
          String runId = StreamEvents.runIdOf(event);
          if (runId != null && !seenStart.add(runId)) {
            continue;
          }
          correlation.correlate(StreamEvents.runOf(event));
        }
        case "run:stop", "run:fail", "run:cancel" -> {
          String runId = StreamEvents.runIdOf(event);
          if (runId != null && !seenTerminal.add(runId)) {
            continue;
          }
          Map<String, Object> run = StreamEvents.runOf(event);
          terminal =
              correlation.correlate(run)
                  && !("run:fail".equals(StreamEvents.typeOf(event))
                      && correlation.superseded(run));
        }
        default -> {}
      }

      delivered = true;
      nextEvent = event;
      return;
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
    SseEvents current = source;
    source = null;
    if (current != null) {
      current.close();
    }
  }
}
