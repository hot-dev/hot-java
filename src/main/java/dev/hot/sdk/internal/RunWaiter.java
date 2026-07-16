package dev.hot.sdk.internal;

import dev.hot.sdk.HotRunException;
import dev.hot.sdk.HotStreamException;
import dev.hot.sdk.HotTimeoutException;
import dev.hot.sdk.StreamEvents;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** Waits on a stream for a run's terminal event. Not part of the public API. */
public final class RunWaiter {

  // A daemon watchdog closes the stream at the deadline so a silent stream
  // cannot block past the timeout; the per-event check below handles streams
  // that keep trickling events.
  private static final ScheduledExecutorService WATCHDOG =
      Executors.newSingleThreadScheduledExecutor(
          runnable -> {
            Thread thread = new Thread(runnable, "hot-sdk-wait-watchdog");
            thread.setDaemon(true);
            return thread;
          });

  private RunWaiter() {}

  public static Map<String, Object> waitForRunResult(
      Transport transport,
      String streamId,
      String eventId,
      Duration timeout,
      Consumer<String> onChunk) {
    long deadline = System.nanoTime() + timeout.toNanos();

    try (StreamEvents events =
        transport.stream(
            "GET", "/streams/" + Transport.enc(streamId) + "/subscribe", null, null)) {
      ScheduledFuture<?> closer =
          WATCHDOG.schedule(events::close, timeout.toNanos(), TimeUnit.NANOSECONDS);
      try {
        return waitOnEvents(events, eventId, deadline, onChunk);
      } catch (RuntimeException error) {
        if (System.nanoTime() > deadline) {
          throw new HotTimeoutException("Timeout waiting for run result");
        }
        throw error;
      } finally {
        closer.cancel(false);
      }
    }
  }

  private static Map<String, Object> waitOnEvents(
      StreamEvents events, String eventId, long deadline, Consumer<String> onChunk) {
    String currentRunId = null;
    while (events.hasNext()) {
      Map<String, Object> event = events.next();
      if (System.nanoTime() > deadline) {
        throw new HotTimeoutException("Timeout waiting for run result");
      }

      String type = StreamEvents.typeOf(event);
      Map<String, Object> run = StreamEvents.runOf(event);

      if (type.equals("run:start") && run != null && eventId.equals(run.get("event_id"))) {
        currentRunId = StreamEvents.runIdOf(event);
      }

      if (type.equals("stream:data")
          && currentRunId != null
          && currentRunId.equals(event.get("run_id"))
          && onChunk != null
          && event.get("payload") instanceof Map<?, ?> payload
          && payload.get("text") instanceof String text) {
        onChunk.accept(text);
      }

      if (type.equals("run:stop") || type.equals("run:fail") || type.equals("run:cancel")) {
        if (run == null || !eventId.equals(run.get("event_id"))) {
          continue;
        }
        if (type.equals("run:fail")) {
          throw new HotRunException(runResultMessage(run, "Run failed"));
        }
        if (type.equals("run:cancel")) {
          throw new HotRunException(runResultMessage(run, "Run cancelled"));
        }
        return run;
      }
    }

    throw new HotStreamException("Stream ended before run completed");
  }

  /** Digs a human-readable message out of a run result value. */
  private static String resultMessage(Object value) {
    if (value instanceof String text && !text.isEmpty()) {
      return text;
    }
    if (value instanceof Map<?, ?> object) {
      for (String key : List.of("$err", "$val", "error", "message", "msg", "reason", "err")) {
        String message = resultMessage(object.get(key));
        if (message != null) {
          return message;
        }
      }
    }
    return null;
  }

  public static String runResultMessage(Map<String, Object> run, String fallback) {
    String message = resultMessage(run.get("result"));
    if (message != null) {
      return message;
    }
    if (run.get("status") instanceof String status && !status.isEmpty()) {
      return fallback + " (" + status + ")";
    }
    return fallback;
  }

  /** Unwraps a {@code {"$ok": ...}} run result. */
  public static Object extractRunResult(Object result) {
    if (result instanceof Map<?, ?> object && object.containsKey("$ok")) {
      return object.get("$ok");
    }
    return result;
  }
}
