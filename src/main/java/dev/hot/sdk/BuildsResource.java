package dev.hot.sdk;

import dev.hot.sdk.internal.Json;
import dev.hot.sdk.internal.Responses;
import dev.hot.sdk.internal.Multipart;
import dev.hot.sdk.internal.Transport;
import java.io.InputStream;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.util.LinkedHashMap;
import java.util.Map;

/** Uploads, deploys, and inspects builds. */
public final class BuildsResource {

  private final Transport transport;

  BuildsResource(Transport transport) {
    this.transport = transport;
  }

  /** Returns the build list envelope across all projects. */
  public Map<String, Object> list() {
    return list(null);
  }

  /** Returns the build list envelope filtered by query parameters. */
  public Map<String, Object> list(Map<String, String> query) {
    return transport.requestJson("GET", "/builds", null, query);
  }

  /** Returns the build list envelope for one project. */
  public Map<String, Object> listForProject(String project) {
    return listForProject(project, null);
  }

  /** Returns the build list envelope for one project, filtered by query parameters. */
  public Map<String, Object> listForProject(String project, Map<String, String> query) {
    return transport.requestJson(
        "GET", "/projects/" + Transport.enc(project) + "/builds", null, query);
  }

  /** Returns a single build. */
  public Map<String, Object> get(String project, String buildId) {
    return Json.dataObject(
        transport.requestJson(
            "GET",
            "/projects/" + Transport.enc(project) + "/builds/" + Transport.enc(buildId),
            null,
            null));
  }

  /** Returns the project's deployed build. */
  public Map<String, Object> deployed(String project) {
    return Json.dataObject(
        transport.requestJson(
            "GET", "/projects/" + Transport.enc(project) + "/builds/deployed", null, null));
  }

  /** Returns the project's live build. */
  public Map<String, Object> live(String project) {
    return Json.dataObject(
        transport.requestJson(
            "GET", "/projects/" + Transport.enc(project) + "/builds/live", null, null));
  }

  /** Uploads a build archive as multipart form data. */
  public Map<String, Object> upload(String project, BuildUpload upload) {
    Map<String, String> fields = new LinkedHashMap<>();
    fields.put("hash", upload.hash());
    if (upload.buildId() != null) {
      fields.put("build_id", upload.buildId());
    }
    Multipart.Encoded encoded =
        Multipart.encode(fields, "file", upload.filename(), upload.content());

    HttpResponse<InputStream> response =
        transport.requestRaw(
            "POST",
            "/projects/" + Transport.enc(project) + "/builds",
            null,
            Map.of("Accept", "application/json", "Content-Type", encoded.contentType()),
            BodyPublishers.ofByteArray(encoded.body()));
    return Responses.readData(response);
  }

  /**
   * Returns the raw HTTP response streaming the build archive. The caller must close the body
   * stream.
   */
  public HttpResponse<InputStream> download(String project, String buildId) {
    return transport.requestRaw(
        "GET",
        "/projects/" + Transport.enc(project) + "/builds/" + Transport.enc(buildId) + "/download",
        null,
        null,
        BodyPublishers.noBody());
  }

  /** Deploys a build. */
  public Map<String, Object> deploy(String project, String buildId) {
    return Json.dataObject(
        transport.requestJson(
            "POST",
            "/projects/" + Transport.enc(project) + "/builds/" + Transport.enc(buildId) + "/deploy",
            null,
            null));
  }
}
