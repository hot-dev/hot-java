package dev.hot.sdk.internal;

import dev.hot.sdk.HotStreamException;
import dev.hot.sdk.HotTransportException;
import dev.hot.sdk.StreamEvents;
import dev.hot.sdk.SubscribeWithEventOptions;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * subscribeWithEvent's reconnect loop: resubscribes to the same stream_id across the API's
 * 5-minute SSE timeout until a terminal run event arrives, deduping replayed run:start and
 * terminal events by run_id. Not part of the public API.
 */
public final class ReconnectingEvents implements StreamEvents {

  private final Transport transport;
  private final Map<String, Object> body;
  private final SubscribeWithEventOptions options;
  private final Set<String> seenStart = new HashSet<>();
  private final Set<String> seenTerminal = new HashSet<>();

  private StreamEvents source;
  private Map<String, Object> nextEvent;
  private String streamId;
  private String eventId;
  private int attempts;
  private boolean terminal;
  private boolean done;

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
    if (source != null) {
      source.close();
      source = null;
    }
  }

  private void advance() {
    while (!done) {
      if (source == null) {
        source = open();
      }

      Map<String, Object> event;
      try {
        if (!source.hasNext()) {
          onConnectionEnd();
          continue;
        }
        event = source.next();
      } catch (RuntimeException error) {
        if (streamId == null || attempts >= options.maxAttempts()) {
          done = true;
          throw error;
        }
        closeSource();
        backoff();
        continue;
      }

      switch (StreamEvents.typeOf(event)) {
        case "event:published" -> {
          if (event.get("stream_id") instanceof String published) {
            streamId = published;
          }
          if (event.get("event_id") instanceof String published) {
            eventId = published;
          }
        }
        case "run:start" -> {
          String runId = StreamEvents.runIdOf(event);
          if (runId != null && !seenStart.add(runId)) {
            continue;
          }
        }
        case "run:stop", "run:fail", "run:cancel" -> {
          String runId = StreamEvents.runIdOf(event);
          if (runId != null && !seenTerminal.add(runId)) {
            continue;
          }
          Map<String, Object> run = StreamEvents.runOf(event);
          terminal = run != null && eventId != null && eventId.equals(run.get("event_id"));
        }
        default -> {}
      }

      nextEvent = event;
      return;
    }
  }

  private StreamEvents open() {
    Map<String, String> query = new HashMap<>();
    if (options.project() != null) {
      query.put("project", options.project());
    }
    if (streamId == null) {
      return transport.stream("POST", "/streams/subscribe-with-event", body, query);
    }
    return transport.stream(
        "GET", "/streams/" + Transport.enc(streamId) + "/subscribe", null, query);
  }

  private void onConnectionEnd() {
    closeSource();
    if (terminal) {
      done = true;
      return;
    }
    if (streamId == null) {
      done = true;
      throw new HotStreamException("stream ended before event:published was received");
    }
    if (attempts >= options.maxAttempts()) {
      done = true;
      throw new HotStreamException(
          "stream ended before the published event's run completed");
    }
    backoff();
  }

  private void closeSource() {
    if (source != null) {
      source.close();
      source = null;
    }
  }

  private void backoff() {
    attempts++;
    long delayMillis = Math.min(250L * attempts, 2000L);
    try {
      Thread.sleep(delayMillis);
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      done = true;
      throw new HotTransportException(error);
    }
  }
}
