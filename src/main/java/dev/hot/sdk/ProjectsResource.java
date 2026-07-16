package dev.hot.sdk;

import dev.hot.sdk.internal.Json;
import dev.hot.sdk.internal.Responses;
import dev.hot.sdk.internal.Transport;
import java.net.http.HttpRequest.BodyPublishers;
import java.util.Map;

/** Manages projects. */
public final class ProjectsResource {

  private final Transport transport;

  ProjectsResource(Transport transport) {
    this.transport = transport;
  }

  /** Returns the project list envelope (data + pagination). */
  public Map<String, Object> list() {
    return list(null);
  }

  /** Returns the project list envelope filtered by query parameters. */
  public Map<String, Object> list(Map<String, String> query) {
    return transport.requestJson("GET", "/projects", null, query);
  }

  /** Creates a project. */
  public Map<String, Object> create(Map<String, Object> body) {
    return Json.dataObject(transport.requestJson("POST", "/projects", body, null));
  }

  /** Returns a project by id or slug. */
  public Map<String, Object> get(String project) {
    return Json.dataObject(
        transport.requestJson("GET", "/projects/" + Transport.enc(project), null, null));
  }

  /** Patches a project. */
  public Map<String, Object> update(String project, Map<String, Object> body) {
    return Json.dataObject(
        transport.requestJson("PATCH", "/projects/" + Transport.enc(project), body, null));
  }

  /** Deletes a project. */
  public void delete(String project) {
    Responses.closeQuietly(
        transport.requestRaw(
            "DELETE", "/projects/" + Transport.enc(project), null, null,
            BodyPublishers.noBody()));
  }

  /** Activates a project. */
  public Map<String, Object> activate(String project) {
    return Json.dataObject(
        transport.requestJson(
            "POST", "/projects/" + Transport.enc(project) + "/activate", null, null));
  }

  /** Deactivates a project. */
  public Map<String, Object> deactivate(String project) {
    return Json.dataObject(
        transport.requestJson(
            "POST", "/projects/" + Transport.enc(project) + "/deactivate", null, null));
  }

  /** Returns the project's event handlers. */
  public Map<String, Object> eventHandlers(String project) {
    return transport.requestJson(
        "GET", "/projects/" + Transport.enc(project) + "/event-handlers", null, null);
  }

  /** Returns the project's schedules. */
  public Map<String, Object> schedules(String project) {
    return transport.requestJson(
        "GET", "/projects/" + Transport.enc(project) + "/schedules", null, null);
  }
}
