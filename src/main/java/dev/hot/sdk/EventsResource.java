package dev.hot.sdk;

import dev.hot.sdk.internal.Json;
import dev.hot.sdk.internal.RunWaiter;
import dev.hot.sdk.internal.Transport;
import java.util.List;
import java.util.Map;

/** Publishes and inspects Hot events. */
public final class EventsResource {

  private final Transport transport;

  EventsResource(Transport transport) {
    this.transport = transport;
  }

  /** Publishes an event and returns the created event data (event_id, stream_id, ...). */
  public Map<String, Object> publish(Map<String, Object> body) {
    return Json.dataObject(transport.requestJson("POST", "/events", body, null));
  }

  /** Calls a Hot function and waits for its result with default options. */
  public Object callHot(String fn, List<Object> args) {
    return callHot(fn, args, new CallOptions());
  }

  /**
   * Publishes a hot:call event for {@code fn} with {@code args} and waits for the run result,
   * unwrapping a {@code {"$ok": ...}} result value.
   */
  public Object callHot(String fn, List<Object> args, CallOptions options) {
    Map<String, Object> run =
        RunWaiter.waitForCallResult(
            transport,
            fn,
            args == null ? List.of() : args,
            options.timeout(),
            options.onChunk(),
            options.maxAttempts());
    return RunWaiter.extractRunResult(run.get("result"));
  }

  /** Returns the event list envelope (data + pagination). */
  public Map<String, Object> list() {
    return list(null);
  }

  /** Returns the event list envelope filtered by query parameters. */
  public Map<String, Object> list(Map<String, String> query) {
    return transport.requestJson("GET", "/events", null, query);
  }

  /** Returns a single event. */
  public Map<String, Object> get(String eventId) {
    return Json.dataObject(
        transport.requestJson("GET", "/events/" + Transport.enc(eventId), null, null));
  }

  /** Returns the runs triggered by an event. */
  public Map<String, Object> getRuns(String eventId) {
    return transport.requestJson("GET", "/events/" + Transport.enc(eventId) + "/runs", null, null);
  }
}
