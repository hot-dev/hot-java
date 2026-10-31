package dev.hot.sdk;

import dev.hot.sdk.internal.Transport;
import java.io.InputStream;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;

/**
 * Downloads large payloads that Hot spilled to blob storage (environment API keys only).
 *
 * <p>A payload too large to inline in a response is replaced by a {@code blob:{blob_ref_id}}
 * reference. The key needs {@code read} on {@code blob:{blob_ref_id}}, and the blob must be in the
 * caller's environment. This route is not available through {@link HotClient#orgs()}.
 */
public final class BlobsResource {

  private final Transport transport;

  BlobsResource(Transport transport) {
    this.transport = transport;
  }

  /**
   * Returns the raw HTTP response streaming the blob's bytes ({@code application/octet-stream};
   * string payloads are served as UTF-8 text). The caller must close the body stream.
   */
  public HttpResponse<InputStream> download(String blobRefId) {
    return transport.requestRaw(
        "GET", "/blobs/" + Transport.enc(blobRefId) + "/download", null, null,
        BodyPublishers.noBody());
  }
}
