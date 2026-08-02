package dev.hot.sdk.internal;

import dev.hot.sdk.HotRunException;
import dev.hot.sdk.HotStreamException;
import dev.hot.sdk.HotTaskException;
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
    RunState state = new RunState(eventId);
    for (int attempt = 0; attempt <= 5; attempt++) {
      try (StreamEvents events =
          transport.stream(
              "GET", "/streams/" + Transport.enc(streamId) + "/subscribe", null, null)) {
        Map<String, Object> run = waitOnConnection(events, state, deadline, onChunk, false);
        if (run != null) {
          return run;
        }
      } catch (HotRunException error) {
        throw error;
      } catch (RuntimeException error) {
        if (expired(deadline)) {
          throw new HotTimeoutException("Timeout waiting for run result");
        }
        if (attempt >= 5) {
          throw error;
        }
      }
      if (expired(deadline)) {
        throw new HotTimeoutException("Timeout waiting for run result");
      }
      if (attempt >= 5) {
        throw new HotStreamException("Stream ended before run completed");
      }
    }
    throw new AssertionError();
  }

  public static Map<String, Object> waitForCallResult(
      Transport transport,
      String fn,
      List<Object> args,
      Duration timeout,
      Consumer<String> onChunk) {
    long deadline = System.nanoTime() + timeout.toNanos();
    RunState state = new RunState(null);
    Map<String, Object> body =
        Map.of(
            "event_type", "hot:call",
            "event_data", Map.of("fn", fn, "args", args));
    for (int attempt = 0; attempt <= 5; attempt++) {
      String method = state.streamId == null ? "POST" : "GET";
      String path =
          state.streamId == null
              ? "/streams/subscribe-with-event"
              : "/streams/" + Transport.enc(state.streamId) + "/subscribe";
      Object requestBody = state.streamId == null ? body : null;
      try (StreamEvents events = transport.stream(method, path, requestBody, null)) {
        Map<String, Object> run = waitOnConnection(events, state, deadline, onChunk, true);
        if (run != null) {
          return run;
        }
      } catch (HotRunException error) {
        throw error;
      } catch (RuntimeException error) {
        if (expired(deadline)) {
          throw new HotTimeoutException("Timeout waiting for run result");
        }
        if (state.streamId == null || attempt >= 5) {
          throw error;
        }
      }
      if (state.streamId == null || state.eventId == null) {
        throw new HotStreamException("stream ended before event:published was received");
      }
      if (expired(deadline)) {
        throw new HotTimeoutException("Timeout waiting for run result");
      }
      if (attempt >= 5) {
        throw new HotStreamException("Stream ended before run completed");
      }
    }
    throw new AssertionError();
  }

  public static Map<String, Object> waitForTask(
      Transport transport, String taskId, Duration timeout, int maxAttempts) {
    long deadline = System.nanoTime() + timeout.toNanos();
    for (int attempt = 0; attempt <= maxAttempts; attempt++) {
      try (StreamEvents events =
          transport.stream(
              "GET", "/tasks/" + Transport.enc(taskId) + "/subscribe", null, null)) {
        ScheduledFuture<?> closer = scheduleClose(events, deadline);
        try {
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
            Object status = task.get("status");
            if ("completed".equals(status)) {
              return task;
            }
            if ("failed".equals(status)
                || "cancelled".equals(status)
                || "timed_out".equals(status)) {
              String message = resultMessage(task.get("result"));
              throw new HotTaskException(
                  message != null ? message : "Task " + status, task);
            }
          }
        } finally {
          closer.cancel(false);
        }
      } catch (HotTaskException error) {
        throw error;
      } catch (RuntimeException error) {
        if (expired(deadline)) {
          throw new HotTimeoutException("Timeout waiting for task");
        }
        if (attempt >= maxAttempts) {
          throw error;
        }
      }
      if (expired(deadline)) {
        throw new HotTimeoutException("Timeout waiting for task");
      }
      if (attempt >= maxAttempts) {
        throw new HotStreamException("Task subscription ended before the task completed");
      }
    }
    throw new AssertionError();
  }

  public static Map<String, Object> waitForRun(
      Transport transport, String runId, Duration timeout, int maxAttempts) {
    long deadline = System.nanoTime() + timeout.toNanos();
    for (int attempt = 0; attempt <= maxAttempts; attempt++) {
      try (StreamEvents events =
          transport.stream(
              "GET", "/runs/" + Transport.enc(runId) + "/subscribe", null, null)) {
        ScheduledFuture<?> closer = scheduleClose(events, deadline);
        try {
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
            Object status = run.get("status");
            if ("succeeded".equals(status)) {
              return run;
            }
            if ("failed".equals(status) || "cancelled".equals(status)) {
              throw new HotRunException(runResultMessage(run, "Run " + status), run);
            }
          }
        } finally {
          closer.cancel(false);
        }
      } catch (HotRunException error) {
        throw error;
      } catch (RuntimeException error) {
        if (expired(deadline)) {
          throw new HotTimeoutException("Timeout waiting for run");
        }
        if (attempt >= maxAttempts) {
          throw error;
        }
      }
      if (expired(deadline)) {
        throw new HotTimeoutException("Timeout waiting for run");
      }
      if (attempt >= maxAttempts) {
        throw new HotStreamException("Run subscription ended before the run completed");
      }
    }
    throw new AssertionError();
  }

  private static Map<String, Object> waitOnConnection(
      StreamEvents events,
      RunState state,
      long deadline,
      Consumer<String> onChunk,
      boolean capturePublished) {
    ScheduledFuture<?> closer = scheduleClose(events, deadline);
    try {
      while (events.hasNext()) {
        Map<String, Object> event = events.next();
        if (expired(deadline)) {
          throw new HotTimeoutException("Timeout waiting for run result");
        }

        String type = StreamEvents.typeOf(event);
        if (capturePublished && type.equals("event:published")) {
          if (event.get("stream_id") instanceof String streamId) {
            state.streamId = streamId;
          }
          if (event.get("event_id") instanceof String eventId) {
            state.eventId = eventId;
          }
        }

        Map<String, Object> run = StreamEvents.runOf(event);

        if (type.equals("run:start")
            && run != null
            && state.eventId != null
            && state.eventId.equals(run.get("event_id"))) {
          state.currentRunId = StreamEvents.runIdOf(event);
        }

        if (type.equals("stream:data")
            && state.currentRunId != null
            && state.currentRunId.equals(event.get("run_id"))
            && onChunk != null
            && event.get("payload") instanceof Map<?, ?> payload
            && payload.get("text") instanceof String text) {
          onChunk.accept(text);
        }

        if (type.equals("run:stop") || type.equals("run:fail") || type.equals("run:cancel")) {
          if (run == null || state.eventId == null || !state.eventId.equals(run.get("event_id"))) {
            continue;
          }
          if (type.equals("run:fail")) {
            throw new HotRunException(runResultMessage(run, "Run failed"), run);
          }
          if (type.equals("run:cancel")) {
            throw new HotRunException(runResultMessage(run, "Run cancelled"), run);
          }
          return run;
        }
      }
      return null;
    } finally {
      closer.cancel(false);
    }
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
    private String eventId;
    private String currentRunId;

    private RunState(String eventId) {
      this.eventId = eventId;
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
