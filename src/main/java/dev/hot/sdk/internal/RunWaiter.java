package dev.hot.sdk.internal;

import dev.hot.sdk.HotRunException;
import dev.hot.sdk.HotStreamException;
import dev.hot.sdk.HotTaskException;
import dev.hot.sdk.HotTimeoutException;
import dev.hot.sdk.HotTransportException;
import dev.hot.sdk.StreamEvents;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Waits on a stream for a run's or task's terminal event, reconnecting per {@link Backoff}. Not
 * part of the public API.
 *
 * <p>The caller's timeout is an absolute deadline. It bounds the wait for response headers (as
 * the request timeout), the body (a watchdog closes the stream at the deadline, and every event
 * is checked against it) and the backoff between connections.
 */
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

  /** No line (keepalives arrive every 15 s) for this long means a dead connection. */
  public static final Duration IDLE_TIMEOUT = Duration.ofSeconds(45);

  public static final int DEFAULT_MAX_ATTEMPTS = 5;

  private static final String RUN_RESULT_TIMEOUT = "Timeout waiting for run result";

  private RunWaiter() {}

  public static Map<String, Object> waitForRunResult(
      Transport transport,
      String streamId,
      String eventId,
      Duration timeout,
      Consumer<String> onChunk) {
    return waitForRunResult(
        transport, streamId, eventId, timeout, onChunk, DEFAULT_MAX_ATTEMPTS);
  }

  public static Map<String, Object> waitForRunResult(
      Transport transport,
      String streamId,
      String eventId,
      Duration timeout,
      Consumer<String> onChunk,
      int maxAttempts) {
    RunState state = new RunState(eventId);
    state.streamId = streamId;
    return loop(
        deadline(timeout),
        maxAttempts,
        RUN_RESULT_TIMEOUT,
        "Stream ended before run completed",
        new Connection<>() {
          @Override
          public SseEvents open(Duration headerTimeout) {
            return transport.stream(
                "GET",
                "/streams/" + Transport.enc(streamId) + "/subscribe",
                null,
                null,
                headerTimeout,
                IDLE_TIMEOUT);
          }

          @Override
          public Map<String, Object> consume(SseEvents events, long deadline, Progress progress) {
            return consumeRun(events, state, deadline, onChunk, false, progress);
          }
        });
  }

  public static Map<String, Object> waitForCallResult(
      Transport transport,
      String fn,
      List<Object> args,
      Duration timeout,
      Consumer<String> onChunk) {
    return waitForCallResult(transport, fn, args, timeout, onChunk, DEFAULT_MAX_ATTEMPTS);
  }

  public static Map<String, Object> waitForCallResult(
      Transport transport,
      String fn,
      List<Object> args,
      Duration timeout,
      Consumer<String> onChunk,
      int maxAttempts) {
    RunState state = new RunState(null);
    Map<String, Object> body =
        Map.of("event_type", "hot:call", "event_data", Map.of("fn", fn, "args", args));
    return loop(
        deadline(timeout),
        maxAttempts,
        RUN_RESULT_TIMEOUT,
        "Stream ended before run completed",
        new Connection<>() {
          private boolean posted;

          @Override
          public SseEvents open(Duration headerTimeout) {
            if (state.streamId == null) {
              // Publishing twice would run the call twice: only the first connection posts.
              if (posted) {
                throw new HotStreamException("stream ended before event:published was received");
              }
              posted = true;
              return transport.stream(
                  "POST",
                  "/streams/subscribe-with-event",
                  body,
                  null,
                  headerTimeout,
                  IDLE_TIMEOUT);
            }
            return transport.stream(
                "GET",
                "/streams/" + Transport.enc(state.streamId) + "/subscribe",
                null,
                null,
                headerTimeout,
                IDLE_TIMEOUT);
          }

          @Override
          public Map<String, Object> consume(SseEvents events, long deadline, Progress progress) {
            return consumeRun(events, state, deadline, onChunk, true, progress);
          }

          @Override
          public boolean canReconnect() {
            return state.streamId != null && state.correlation.eventId() != null;
          }
        });
  }

  public static Map<String, Object> waitForTask(
      Transport transport, String taskId, Duration timeout, int maxAttempts) {
    Map<?, ?>[] last = new Map<?, ?>[1];
    return loop(
        deadline(timeout),
        maxAttempts,
        "Timeout waiting for task",
        "Task subscription ended before the task completed",
        new Connection<>() {
          @Override
          public SseEvents open(Duration headerTimeout) {
            return transport.stream(
                "GET",
                "/tasks/" + Transport.enc(taskId) + "/subscribe",
                null,
                null,
                headerTimeout,
                IDLE_TIMEOUT);
          }

          @Override
          public Map<String, Object> consume(SseEvents events, long deadline, Progress progress) {
            while (events.hasNext()) {
              Map<String, Object> event = events.next();
              if (expired(deadline)) {
                throw new HotTimeoutException("Timeout waiting for task");
              }
              if (!"task:update".equals(StreamEvents.typeOf(event))) {
                continue;
              }
              Map<String, Object> task = objectField(event, "task");
              if (task == null) {
                continue;
              }
              if (!task.equals(last[0])) {
                last[0] = task;
                progress.delivered = true;
              }
              Object status = task.get("status");
              if ("completed".equals(status)) {
                return task;
              }
              if ("failed".equals(status)
                  || "cancelled".equals(status)
                  || "timed_out".equals(status)) {
                String message = resultMessage(task.get("result"));
                throw new HotTaskException(message != null ? message : "Task " + status, task);
              }
            }
            return null;
          }
        });
  }

  public static Map<String, Object> waitForRun(
      Transport transport, String runId, Duration timeout, int maxAttempts) {
    Map<?, ?>[] last = new Map<?, ?>[1];
    return loop(
        deadline(timeout),
        maxAttempts,
        "Timeout waiting for run",
        "Run subscription ended before the run completed",
        new Connection<>() {
          @Override
          public SseEvents open(Duration headerTimeout) {
            return transport.stream(
                "GET",
                "/runs/" + Transport.enc(runId) + "/subscribe",
                null,
                null,
                headerTimeout,
                IDLE_TIMEOUT);
          }

          @Override
          public Map<String, Object> consume(SseEvents events, long deadline, Progress progress) {
            while (events.hasNext()) {
              Map<String, Object> event = events.next();
              if (expired(deadline)) {
                throw new HotTimeoutException("Timeout waiting for run");
              }
              if (!"run:update".equals(StreamEvents.typeOf(event))) {
                continue;
              }
              Map<String, Object> run = objectField(event, "run");
              if (run == null) {
                continue;
              }
              if (!run.equals(last[0])) {
                last[0] = run;
                progress.delivered = true;
              }
              Object status = run.get("status");
              if ("succeeded".equals(status)) {
                return run;
              }
              if ("failed".equals(status) || "cancelled".equals(status)) {
                throw new HotRunException(runResultMessage(run, "Run " + status), run);
              }
            }
            return null;
          }
        });
  }

  /** One SSE connection of a waiter. */
  private interface Connection<T> {
    /** Opens the next connection; {@code headerTimeout} is the time left before the deadline. */
    SseEvents open(Duration headerTimeout);

    /** Reads events until the result (returned), a terminal error (thrown) or the end (null). */
    T consume(SseEvents events, long deadline, Progress progress);

    /** Whether the waiter can resubscribe after this connection ended. */
    default boolean canReconnect() {
      return true;
    }
  }

  /** What one connection achieved, for the consecutive-failure budget. */
  private static final class Progress {
    /** The connection delivered at least one event the waiter had not seen. */
    boolean delivered;
  }

  /**
   * Runs connections until one returns a result. Permanent errors and terminal run/task errors
   * end the wait; anything else reconnects. Only consecutive unproductive connections count
   * against {@code maxAttempts}: a connection that delivered new events or ended with the API's
   * stream:complete rollover resets the count and reconnects at once.
   */
  private static <T> T loop(
      long deadline,
      int maxAttempts,
      String timeoutMessage,
      String endedMessage,
      Connection<T> connection) {
    int failures = 0;
    while (true) {
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0) {
        throw new HotTimeoutException(timeoutMessage);
      }
      Progress progress = new Progress();
      RuntimeException failure = null;
      boolean rolledOver = false;
      try (SseEvents events = connection.open(Duration.ofNanos(remaining))) {
        ScheduledFuture<?> closer = scheduleClose(events, deadline);
        try {
          T result = connection.consume(events, deadline, progress);
          if (result != null) {
            return result;
          }
          rolledOver = events.sawComplete();
        } finally {
          closer.cancel(false);
        }
      } catch (HotRunException | HotTaskException | HotTimeoutException error) {
        throw error;
      } catch (RuntimeException error) {
        if (Backoff.isPermanent(error)) {
          throw error;
        }
        failure = error;
      }
      if (expired(deadline) || hitHeaderTimeout(failure)) {
        throw new HotTimeoutException(timeoutMessage, failure);
      }
      if (!connection.canReconnect()) {
        if (failure != null) {
          throw failure;
        }
        throw new HotStreamException("stream ended before event:published was received");
      }
      if (progress.delivered || rolledOver) {
        failures = 0;
        if (failure == null) {
          continue;
        }
      }
      failures++;
      if (failures > maxAttempts) {
        if (failure != null) {
          throw failure;
        }
        throw new HotStreamException(endedMessage);
      }
      Backoff.sleep(
          Backoff.reconnectDelayMillis(failures, failure), deadline, timeoutMessage, failure);
    }
  }

  private static Map<String, Object> consumeRun(
      SseEvents events,
      RunState state,
      long deadline,
      Consumer<String> onChunk,
      boolean capturePublished,
      Progress progress) {
    RunCorrelation correlation = state.correlation;
    while (events.hasNext()) {
      Map<String, Object> event = events.next();
      if (expired(deadline)) {
        throw new HotTimeoutException(RUN_RESULT_TIMEOUT);
      }

      String type = StreamEvents.typeOf(event);
      if (type.equals("event:published")) {
        if (capturePublished && state.streamId == null) {
          if (event.get("stream_id") instanceof String streamId) {
            state.streamId = streamId;
          }
          if (event.get("event_id") instanceof String eventId) {
            correlation.eventId(eventId);
          }
          progress.delivered = true;
        }
        continue;
      }
      if (type.equals("stream:data")) {
        // stream:data is never replayed, so every chunk is new.
        progress.delivered = true;
        correlation.chunk(event, onChunk);
        continue;
      }
      if (!type.startsWith("run:")) {
        continue;
      }
      Map<String, Object> run = StreamEvents.runOf(event);
      String runId = StreamEvents.runIdOf(event);
      if (runId == null) {
        continue;
      }
      // Run events are replayed on every new connection; only first sightings are progress.
      if (state.seen.add(type + " " + runId)) {
        progress.delivered = true;
      }
      if (!correlation.correlate(run)) {
        continue;
      }
      correlation.flush(run, onChunk);
      switch (type) {
        case "run:stop" -> {
          return run;
        }
        case "run:fail" -> {
          if (!correlation.superseded(run)) {
            throw new HotRunException(runResultMessage(run, "Run failed"), run);
          }
        }
        case "run:cancel" -> throw new HotRunException(runResultMessage(run, "Run cancelled"), run);
        default -> {}
      }
    }
    return null;
  }

  /**
   * Whether {@code failure} is the request timeout, which every waiter sets to the time left
   * before its deadline. A connect timeout is an ordinary transient failure.
   */
  private static boolean hitHeaderTimeout(RuntimeException failure) {
    return failure instanceof HotTransportException
        && failure.getCause() instanceof HttpTimeoutException
        && !(failure.getCause() instanceof HttpConnectTimeoutException);
  }

  private static long deadline(Duration timeout) {
    return System.nanoTime() + timeout.toNanos();
  }

  private static ScheduledFuture<?> scheduleClose(StreamEvents events, long deadline) {
    long remaining = Math.max(0, deadline - System.nanoTime());
    return WATCHDOG.schedule(events::close, remaining, TimeUnit.NANOSECONDS);
  }

  private static boolean expired(long deadline) {
    return System.nanoTime() >= deadline;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> objectField(Map<String, Object> source, String key) {
    return source.get(key) instanceof Map<?, ?> value ? (Map<String, Object>) value : null;
  }

  private static final class RunState {
    private String streamId;
    private final RunCorrelation correlation;
    private final Set<String> seen = new HashSet<>();

    private RunState(String eventId) {
      this.correlation = new RunCorrelation(eventId);
    }
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
