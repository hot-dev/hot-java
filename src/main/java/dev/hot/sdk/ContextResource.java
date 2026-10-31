package dev.hot.sdk;

import dev.hot.sdk.internal.Json;
import dev.hot.sdk.internal.Responses;
import dev.hot.sdk.internal.Transport;
import java.net.http.HttpRequest.BodyPublishers;
import java.util.Map;

/** Manages encrypted project context variables. */
public final class ContextResource {

  private final Transport transport;
  private final String prefix;

  ContextResource(Transport transport) {
    this(transport, "");
  }

  /** {@code prefix} scopes every path, e.g. {@code /orgs/{org}/envs/{env}}. */
  ContextResource(Transport transport, String prefix) {
    this.transport = transport;
    this.prefix = prefix;
  }

  /** Returns the context variable list envelope for a project. */
  public Map<String, Object> list(String project) {
    return transport.requestJson(
        "GET", projectPath(project) + "/context", null, null);
  }

  /** Creates a context variable. */
  public Map<String, Object> create(String project, Map<String, Object> body) {
    return Json.dataObject(
        transport.requestJson(
            "POST", projectPath(project) + "/context", body, null));
  }

  /** Replaces a context variable. */
  public Map<String, Object> update(String project, String key, Map<String, Object> body) {
    return Json.dataObject(
        transport.requestJson(
            "PUT",
            projectPath(project) + "/context/" + Transport.enc(key),
            body,
            null));
  }

  /** Deletes a context variable. */
  public void delete(String project, String key) {
    Responses.closeQuietly(
        transport.requestRaw(
            "DELETE",
            projectPath(project) + "/context/" + Transport.enc(key),
            null,
            null,
            BodyPublishers.noBody()));
  }

  private String projectPath(String project) {
    return prefix + "/projects/" + Transport.enc(project);
  }
}
