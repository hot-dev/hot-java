package dev.hot.sdk;

import dev.hot.sdk.internal.Json;
import dev.hot.sdk.internal.ReconnectingEvents;
import dev.hot.sdk.internal.RunWaiter;
import dev.hot.sdk.internal.Transport;
import java.util.Map;

/** Subscribes to Hot run streams. */
public final class StreamsResource {

  private final Transport transport;

  StreamsResource(Transport transport) {
    this.transport = transport;
  }

  /** Streams events from an existing run stream. */
  public StreamEvents subscribe(String streamId) {
    return subscribe(streamId, null);
  }

  /** Streams events from an existing run stream, scoped to a project id or slug. */
  public StreamEvents subscribe(String streamId, String project) {
    return transport.stream(
        "GET", "/streams/" + Transport.enc(streamId) + "/subscribe", null, projectQuery(project));
  }

  /** Streams events from an existing run stream via POST. */
  public StreamEvents subscribePost(String streamId) {
    return subscribePost(streamId, null);
  }

  /** Streams events from an existing run stream via POST, scoped to a project id or slug. */
  public StreamEvents subscribePost(String streamId, String project) {
    return transport.stream(
        "POST", "/streams/" + Transport.enc(streamId) + "/subscribe", null, projectQuery(project));
  }

  /**
   * Deletes the call traces of every run on a stream. Returns {@code {calls_deleted,
   * blob_refs_released, complete}}.
   */
  public Map<String, Object> purgeCalls(String streamId) {
    return Json.dataObject(
        transport.requestJson(
            "DELETE", "/streams/" + Transport.enc(streamId) + "/calls", null, null));
  }

  /** Waits for the run created by {@code eventId} with default options. */
  public Map<String, Object> waitForRunResult(String streamId, String eventId) {
    return waitForRunResult(streamId, eventId, new WaitOptions());
  }

  /**
   * Subscribes to a stream and returns the run object once the run created by {@code eventId}
   * reaches run:stop. run:fail and run:cancel become {@link HotRunException} carrying the run's
   * result message; a failure that is being retried instead waits for the retry run.
   */
  public Map<String, Object> waitForRunResult(String streamId, String eventId, WaitOptions options) {
    return RunWaiter.waitForRunResult(
        transport, streamId, eventId, options.timeout(), options.onChunk(), options.maxAttempts());
  }

  /** Atomically subscribes then publishes an event, with default options. */
  public StreamEvents subscribeWithEvent(Map<String, Object> body) {
    return subscribeWithEvent(body, new SubscribeWithEventOptions());
  }

  /**
   * Atomically subscribes then publishes an event, yielding stream events until the run
   * completes.
   *
   * <p>The Hot API closes every SSE subscription 5 minutes after it opens, with stream:complete.
   * While reconnect is enabled (the default), the SDK resubscribes to the same stream_id and
   * keeps yielding events until the published event's final run:stop/run:fail/run:cancel.
   * Events of a retried attempt (status pending_retry, or a replayed failure whose retry_attempt
   * shows a scheduled retry) are yielded but do not end the iterator: the retry run, linked by
   * origin_run_id, does. A failure that announced a retry ends the iterator only when no retry
   * run appears within 5 seconds. Replayed run:start and terminal events are deduped by run_id;
   * stream:data chunks are not replayed and may be lost across the reconnect window.
   */
  public StreamEvents subscribeWithEvent(
      Map<String, Object> body, SubscribeWithEventOptions options) {
    if (!options.reconnectEnabled()) {
      return transport.stream(
          "POST", "/streams/subscribe-with-event", body, projectQuery(options.project()));
    }
    return new ReconnectingEvents(transport, body, options);
  }

  private static Map<String, String> projectQuery(String project) {
    return project == null ? null : Map.of("project", project);
  }
}
