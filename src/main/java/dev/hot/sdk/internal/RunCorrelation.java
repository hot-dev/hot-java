package dev.hot.sdk.internal;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Tracks which runs on a stream belong to one published event. Not part of the public API.
 *
 * <p>A run belongs to the event when its {@code event_id} matches, or when its {@code
 * origin_run_id} names a run that already belongs: a handler's retry runs get a new event id and
 * link back only through {@code origin_run_id}. A {@code run:fail} whose run is {@code
 * pending_retry}, or a run a later attempt superseded, is not the final result.
 *
 * <p>The API can deliver {@code stream:data} (and even {@code run:stop}) before {@code
 * run:start}, so text chunks are buffered per run until that run is correlated, then delivered in
 * order.
 */
final class RunCorrelation {

  // Bounds memory for chunks of runs that never correlate (other events on a shared stream).
  private static final int MAX_BUFFERED_CHUNKS = 10_000;

  private String eventId;
  private final Set<String> runs = new HashSet<>();
  private final Set<String> retried = new HashSet<>();
  private final Map<String, List<String>> buffered = new LinkedHashMap<>();
  private int bufferedCount;

  RunCorrelation(String eventId) {
    this.eventId = eventId;
  }

  String eventId() {
    return eventId;
  }

  void eventId(String eventId) {
    this.eventId = eventId;
  }

  /** Records and returns whether {@code run} belongs to the published event. */
  boolean correlate(Map<String, Object> run) {
    if (run == null || !(run.get("run_id") instanceof String runId)) {
      return false;
    }
    if (runs.contains(runId)) {
      return true;
    }
    boolean direct = eventId != null && eventId.equals(run.get("event_id"));
    boolean retry = run.get("origin_run_id") instanceof String origin && runs.contains(origin);
    if (!direct && !retry) {
      return false;
    }
    runs.add(runId);
    if (retry) {
      retried.add((String) run.get("origin_run_id"));
    }
    if ("pending_retry".equals(run.get("status"))) {
      retried.add(runId);
    }
    return true;
  }

  /**
   * Whether a correlated run's failure is superseded by a retry: it is pending_retry now, or was
   * when first seen (a replay then reports its final status), or a retry run already followed.
   */
  boolean superseded(Map<String, Object> run) {
    if ("pending_retry".equals(run.get("status"))) {
      return true;
    }
    return run.get("run_id") instanceof String runId && retried.contains(runId);
  }

  /** Delivers a stream:data text chunk now if its run is correlated, else buffers it. */
  void chunk(Map<String, Object> event, Consumer<String> onChunk) {
    if (onChunk == null
        || !(event.get("run_id") instanceof String runId)
        || !(event.get("payload") instanceof Map<?, ?> payload)
        || !(payload.get("text") instanceof String text)) {
      return;
    }
    if (runs.contains(runId)) {
      onChunk.accept(text);
    } else if (bufferedCount < MAX_BUFFERED_CHUNKS) {
      buffered.computeIfAbsent(runId, ignored -> new ArrayList<>()).add(text);
      bufferedCount++;
    }
  }

  /** Delivers chunks buffered for {@code run} before it was correlated. */
  void flush(Map<String, Object> run, Consumer<String> onChunk) {
    if (onChunk == null || !(run.get("run_id") instanceof String runId)) {
      return;
    }
    List<String> chunks = buffered.remove(runId);
    if (chunks != null) {
      bufferedCount -= chunks.size();
      chunks.forEach(onChunk);
    }
  }
}
