package dev.hot.sdk.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

/** Minimal multipart/form-data encoder (java.net.http has none). Not part of the public API. */
public final class Multipart {

  /** An encoded multipart body and its Content-Type header value. */
  public record Encoded(String contentType, byte[] body) {}

  private Multipart() {}

  public static Encoded encode(
      Map<String, String> fields, String fileField, String filename, byte[] content) {
    String boundary = "----hot-sdk-" + UUID.randomUUID();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      for (Map.Entry<String, String> field : fields.entrySet()) {
        write(out, "--" + boundary + "\r\n");
        write(
            out,
            "Content-Disposition: form-data; name=\"" + field.getKey() + "\"\r\n\r\n");
        write(out, field.getValue() + "\r\n");
      }
      write(out, "--" + boundary + "\r\n");
      write(
          out,
          "Content-Disposition: form-data; name=\""
              + fileField
              + "\"; filename=\""
              + filename.replace("\"", "%22")
              + "\"\r\n");
      write(out, "Content-Type: application/octet-stream\r\n\r\n");
      out.write(content);
      write(out, "\r\n--" + boundary + "--\r\n");
    } catch (IOException error) {
      throw new UncheckedIOException(error);
    }
    return new Encoded("multipart/form-data; boundary=" + boundary, out.toByteArray());
  }

  private static void write(ByteArrayOutputStream out, String text) throws IOException {
    out.write(text.getBytes(StandardCharsets.UTF_8));
  }
}
