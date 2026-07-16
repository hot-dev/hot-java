package dev.hot.sdk;

import dev.hot.sdk.internal.Json;
import dev.hot.sdk.internal.Responses;
import dev.hot.sdk.internal.Transport;
import java.io.InputStream;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.util.LinkedHashMap;
import java.util.Map;

/** Uploads, downloads, lists, and deletes files. */
public final class FilesResource {

  private final Transport transport;

  FilesResource(Transport transport) {
    this.transport = transport;
  }

  /** Returns the file list envelope (data + pagination). */
  public Map<String, Object> list() {
    return list(null);
  }

  /** Returns the file list envelope filtered by query parameters. */
  public Map<String, Object> list(Map<String, String> query) {
    return transport.requestJson("GET", "/files", null, query);
  }

  /** Returns a single file's metadata. */
  public Map<String, Object> get(String fileId) {
    return Json.dataObject(
        transport.requestJson("GET", "/files/" + Transport.enc(fileId), null, null));
  }

  /** Deletes a file. */
  public void delete(String fileId) {
    Responses.closeQuietly(
        transport.requestRaw(
            "DELETE", "/files/" + Transport.enc(fileId), null, null, BodyPublishers.noBody()));
  }

  /**
   * Returns the raw HTTP response streaming the file content. The caller must close the body
   * stream.
   */
  public HttpResponse<InputStream> download(String fileId) {
    return transport.requestRaw(
        "GET", "/files/" + Transport.enc(fileId) + "/download", null, null,
        BodyPublishers.noBody());
  }

  /** Uploads content to a slash-separated file path. */
  public Map<String, Object> upload(String path, byte[] content) {
    return upload(path, content, null);
  }

  /** Uploads content with an explicit Content-Type. */
  public Map<String, Object> upload(String path, byte[] content, String contentType) {
    Map<String, String> headers = new LinkedHashMap<>();
    headers.put("Accept", "application/json");
    if (contentType != null) {
      headers.put("Content-Type", contentType);
    }
    HttpResponse<InputStream> response =
        transport.requestRaw(
            "PUT",
            "/files/upload/" + Transport.encPath(path),
            null,
            headers,
            BodyPublishers.ofByteArray(content));
    return Responses.readData(response);
  }

  /** Starts a multipart upload. */
  public Map<String, Object> initiateUpload(Map<String, Object> body) {
    return Json.dataObject(transport.requestJson("POST", "/files/uploads", body, null));
  }

  /** Uploads one part of a multipart upload. */
  public Map<String, Object> uploadPart(String uploadId, int partNumber, byte[] content) {
    HttpResponse<InputStream> response =
        transport.requestRaw(
            "PUT",
            "/files/uploads/" + Transport.enc(uploadId) + "/" + partNumber,
            null,
            Map.of("Accept", "application/json"),
            BodyPublishers.ofByteArray(content));
    return Responses.readData(response);
  }

  /** Finishes a multipart upload. */
  public Map<String, Object> completeUpload(String uploadId) {
    return Json.dataObject(
        transport.requestJson(
            "POST", "/files/uploads/" + Transport.enc(uploadId) + "/complete", null, null));
  }

  /** Cancels a multipart upload. */
  public void abortUpload(String uploadId) {
    Responses.closeQuietly(
        transport.requestRaw(
            "DELETE",
            "/files/uploads/" + Transport.enc(uploadId),
            null,
            null,
            BodyPublishers.noBody()));
  }
}
