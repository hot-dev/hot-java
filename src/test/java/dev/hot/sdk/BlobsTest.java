package dev.hot.sdk;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.tomakehurst.wiremock.WireMockServer;
import java.io.InputStream;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Method, path and returned bytes of the blob download route. */
class BlobsTest {

  private WireMockServer server;

  @BeforeEach
  void start() {
    server = new WireMockServer(options().dynamicPort());
    server.start();
  }

  @AfterEach
  void stop() {
    server.stop();
  }

  private HotClient client() {
    return HotClient.builder("test-token").baseUrl(server.baseUrl()).build();
  }

  @Test
  void downloadGetsTheBlobBytesFromTheBlobRoute() throws Exception {
    byte[] bytes = {0, 1, 2, (byte) 0xff, (byte) 0xfe, 'h', 'o', 't'};
    server.stubFor(
        get(urlEqualTo("/v1/blobs/blob_123/download"))
            .withHeader("Authorization", equalTo("Bearer test-token"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/octet-stream")
                    .withBody(bytes)));

    HttpResponse<InputStream> response = client().blobs().download("blob_123");
    try (InputStream body = response.body()) {
      assertEquals(200, response.statusCode());
      assertArrayEquals(bytes, body.readAllBytes());
    }
    server.verify(1, getRequestedFor(urlEqualTo("/v1/blobs/blob_123/download")));
  }

  @Test
  void downloadPercentEncodesTheBlobRefId() throws Exception {
    server.stubFor(
        get(urlEqualTo("/v1/blobs/a%2Fb%20c/download"))
            .willReturn(aResponse().withStatus(200).withBody("text")));

    try (InputStream body = client().blobs().download("a/b c").body()) {
      assertEquals("text", new String(body.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  @Test
  void downloadMapsErrorStatusesToApiExceptions() {
    server.stubFor(
        get(urlEqualTo("/v1/blobs/missing/download"))
            .willReturn(
                aResponse()
                    .withStatus(404)
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"error\":{\"code\":\"not_found\",\"message\":\"no blob\"}}")));

    HotApiException error =
        assertThrows(HotApiException.class, () -> client().blobs().download("missing"));
    assertEquals(404, error.statusCode());
  }
}
