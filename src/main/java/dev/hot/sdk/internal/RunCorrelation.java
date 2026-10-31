package dev.hot.sdk.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Tracks which runs on a stream belong to one published event and when that event's result is
 * final. Not part of the public API. Mirrors hot-rust's {@code RunTracker}.
 *
 * <p>A run is tied to the event when its {@code event_id} matches, or when its {@code
 * origin_run_id} names a tied run of the same {@code run_type}: a handler's retry runs get a new
 * event id and link back only through {@code origin_run_id}. Task runs a run starts also name it
 * as their origin, but with {@code run_type} "task", so they never tie. A run event whose origin
 * is not tied yet is kept as an orphan keyed by that origin, and replayed once the origin ties
 * (the API's poll replays newest runs first).
 *
 * <p>The API replays a retried first attempt lossily: as {@code run:fail} or even {@code
 * run:stop} with status {@code pending_retry}, or, after the retry is queued but before its run
 * exists, as {@code run:fail} with status {@code failed} and a raised {@code retry_attempt}. So:
 *
 * <ul>
 *   <li>{@code pending_retry} is never final, whatever the event type.
 *   <li>A tied run that another tied run (or an orphan) names as its origin was retried; its
 *       {@code run:fail} is not final.
 *   <li>A tied {@code run:fail} with status {@code failed} and a {@code retry_attempt} above its
 *       base (0, or its origin's highest {@code retry_attempt}) is held for {@link #GRACE_NANOS}: it
 *       stands only if no run naming it as origin appears by then, or by the caller's deadline.
 * </ul>
 *
 * <p>The API can also deliver {@code stream:data} (and even {@code run:stop}) before {@code
 * run:start}, so text chunks are buffered per run until that run ties, then delivered in order.
 */
final class RunCorrelation {

  /** How long a failure that announced a retry waits for the retry run. */
  static final long GRACE_NANOS = TimeUnit.SECONDS.toNanos(5);

  // Bounds memory for chunks of runs that never tie (other events on a shared stream).
  private static final int MAX_BUFFERED_CHUNKS = 10_000;

  /** What a run event means for the published event. */
  enum Kind {
    /** Not final: keep waiting. */
    CONTINUE,
    /** A tied run stopped: {@link Outcome#run} is the result. */
    STOP,
    /** A tied run failed for good or was cancelled: {@link Outcome#run} carries the failure. */
    FAIL
  }

  /** The kind, the run it refers to, and the event type that produced it. */
  record Outcome(Kind kind, String type, Map<String, Object> run) {
    static final Outcome CONTINUE = new Outcome(Kind.CONTINUE, null, null);

    boolean isFinal() {
      return kind != Kind.CONTINUE;
    }
  }

  private record Orphan(String type, Map<String, Object> run) {}

  private record PendingFailure(String runId, long until, Map<String, Object> run) {}

  private final Consumer<String> onChunk;
  private String eventId;
  // Tied runs and the highest retry_attempt seen for each.
  private final Map<String, Long> runs = new HashMap<>();
  // run_type of each tied run (null when the API sent none).
  private final Map<String, Object> runTypes = new HashMap<>();
  // Tied runs that a tied retry run names as its origin.
  private final Set<String> retried = new HashSet<>();
  private final Map<String, List<Orphan>> orphans = new HashMap<>();
  private final Map<String, List<String>> buffered = new LinkedHashMap<>();
  private int bufferedCount;
  private PendingFailure pending;

  RunCorrelation(String eventId, Consumer<String> onChunk) {
    this.eventId = eventId;
    this.onChunk = onChunk;
  }

  String eventId() {
    return eventId;
  }

  void eventId(String eventId) {
    this.eventId = eventId;
  }

  /**
   * When a failure is waiting for its retry, the {@link System#nanoTime()} instant it stands;
   * otherwise null.
   */
  Long wakeAt() {
    return pending == null ? null : pending.until;
  }

  /** Whether a held failure's grace has run out. */
  boolean graceOver() {
    return pending != null && System.nanoTime() - pending.until >= 0;
  }

  /**
   * Ends the grace of a held failure: returns it as a final {@code run:fail} outcome, or {@link
   * Outcome#CONTINUE} when none is held.
   */
  Outcome expire() {
    PendingFailure held = pending;
    pending = null;
    return held == null ? Outcome.CONTINUE : new Outcome(Kind.FAIL, "run:fail", held.run);
  }

  /** Handles a run event ({@code run:start}, {@code run:stop}, {@code run:fail}, {@code run:cancel}). */
  Outcome runEvent(String type, Map<String, Object> run) {
    if (run == null || !(run.get("run_id") instanceof String runId)) {
      return Outcome.CONTINUE;
    }
    String origin = origin(run);
    if (!ties(run)) {
      if (origin != null && !runs.containsKey(origin)) {
        orphans.computeIfAbsent(origin, ignored -> new ArrayList<>()).add(new Orphan(type, run));
      }
      return Outcome.CONTINUE;
    }
    long attempt = retryAttempt(run);
    List<Orphan> waiting = List.of();
    Long seen = runs.get(runId);
    if (seen != null) {
      runs.put(runId, Math.max(seen, attempt));
    } else {
      runs.put(runId, attempt);
      runTypes.put(runId, run.get("run_type"));
      flush(runId);
      List<Orphan> found = orphans.remove(runId);
      if (found != null) {
        waiting = found;
        if (found.stream().anyMatch(orphan -> sameType(orphan.run, runId))) {
          // A retry of this run is already on the stream.
          retried.add(runId);
        }
      }
    }
    if (origin != null) {
      retried.add(origin);
      if (pending != null && pending.runId.equals(origin)) {
        pending = null;
      }
    }
    Outcome outcome = outcome(type, runId, run);
    if (outcome.isFinal()) {
      return outcome;
    }
    for (Orphan orphan : waiting) {
      Outcome replayed = runEvent(orphan.type, orphan.run);
      if (replayed.isFinal()) {
        return replayed;
      }
    }
    return Outcome.CONTINUE;
  }

  private Outcome outcome(String type, String runId, Map<String, Object> run) {
    if ("pending_retry".equals(run.get("status"))) {
      return Outcome.CONTINUE;
    }
    switch (type) {
      case "run:stop":
        return new Outcome(Kind.STOP, type, run);
      case "run:cancel":
        return new Outcome(Kind.FAIL, type, run);
      case "run:fail":
        if (retried.contains(runId)) {
          return Outcome.CONTINUE;
        }
        // retry_attempt goes up when a retry is scheduled; a run's own attempt number is its
        // origin's (0 for the first attempt).
        String origin = origin(run);
        long base = origin != null && runs.containsKey(origin) ? runs.get(origin) : 0;
        if (retryAttempt(run) > base) {
          if (pending == null) {
            pending = new PendingFailure(runId, System.nanoTime() + GRACE_NANOS, run);
          }
          return Outcome.CONTINUE;
        }
        return new Outcome(Kind.FAIL, type, run);
      default:
        return Outcome.CONTINUE;
    }
  }

  private boolean ties(Map<String, Object> run) {
    boolean byEvent = eventId != null && eventId.equals(run.get("event_id"));
    String origin = origin(run);
    return byEvent || (origin != null && runs.containsKey(origin) && sameType(run, origin));
  }

  /** Whether {@code run} has the run_type of the tied run {@code origin}: a retry, not a task. */
  private boolean sameType(Map<String, Object> run, String origin) {
    return Objects.equals(run.get("run_type"), runTypes.get(origin));
  }

  /** Delivers a stream:data text chunk now if its run is tied, else buffers it. */
  void chunk(Map<String, Object> event) {
    if (onChunk == null
        || !(event.get("run_id") instanceof String runId)
        || !(event.get("payload") instanceof Map<?, ?> payload)
        || !(payload.get("text") instanceof String text)) {
      return;
    }
    if (runs.containsKey(runId)) {
      onChunk.accept(text);
    } else if (bufferedCount < MAX_BUFFERED_CHUNKS) {
      buffered.computeIfAbsent(runId, ignored -> new ArrayList<>()).add(text);
      bufferedCount++;
    }
  }

  private void flush(String runId) {
    List<String> chunks = buffered.remove(runId);
    if (chunks != null) {
      bufferedCount -= chunks.size();
      if (onChunk != null) {
        chunks.forEach(onChunk);
      }
    }
  }

  private static String origin(Map<String, Object> run) {
    return run.get("origin_run_id") instanceof String origin ? origin : null;
  }

  private static long retryAttempt(Map<String, Object> run) {
    return run.get("retry_attempt") instanceof Number number ? number.longValue() : 0;
  }
}
