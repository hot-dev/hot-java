package dev.hot.sdk;

import dev.hot.sdk.internal.Json;
import dev.hot.sdk.internal.RunWaiter;
import dev.hot.sdk.internal.Transport;
import java.util.Map;

/** Reads and waits for durable asynchronous tasks. */
public final class TasksResource {

  private final Transport transport;

  TasksResource(Transport transport) {
    this.transport = transport;
  }

  /** Returns the latest durable task snapshot. */
  public Map<String, Object> get(String taskId) {
    return Json.dataObject(
        transport.requestJson("GET", "/tasks/" + Transport.enc(taskId), null, null));
  }

  /** Streams durable task:update snapshots until the task is terminal. */
  public StreamEvents subscribe(String taskId) {
    return transport.stream(
        "GET", "/tasks/" + Transport.enc(taskId) + "/subscribe", null, null);
  }

  public Map<String, Object> waitFor(String taskId) {
    return waitFor(taskId, new TaskWaitOptions());
  }

  /** Waits for successful completion and returns the final durable snapshot. */
  public Map<String, Object> waitFor(String taskId, TaskWaitOptions options) {
    return RunWaiter.waitForTask(
        transport, taskId, options.timeout(), options.maxAttempts());
  }
}
