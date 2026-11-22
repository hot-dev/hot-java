package dev.hot.sdk;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.deleteRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.patch;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import java.io.InputStream;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for FilesResource, BuildsResource, DomainsResource, SessionsResource, ServiceKeysResource. */
class ResourcesTest {

  private WireMockServer server;

  @BeforeEach
  void start() {
    server = new WireMockServer(options().dynamicPort().http2PlainDisabled(true));
    server.start();
  }

  @AfterEach
  void stop() {
    server.stop();
  }

  private HotClient client() {
    return HotClient.builder("test-token").baseUrl(server.baseUrl()).build();
  }

  private static String envelope(String dataJson) {
    return "{\"data\":" + dataJson + ",\"meta\":{}}";
  }

  @Test
  void filesResourceCoversUploadDownloadListGetDeleteAndMultipart() throws Exception {
    byte[] payload = "hello-file".getBytes(StandardCharsets.UTF_8);

    server.stubFor(
        get(urlPathEqualTo("/v1/files"))
            .withQueryParam("prefix", equalTo("docs/"))
            .willReturn(aResponse().withBody("{\"data\":[{\"file_id\":\"f1\"}],\"meta\":{}}")));
    server.stubFor(
        get(urlPathEqualTo("/v1/files/f1"))
            .willReturn(aResponse().withBody(envelope("{\"file_id\":\"f1\"}"))));
    server.stubFor(
        get(urlPathEqualTo("/v1/files/f1/download"))
            .willReturn(aResponse().withStatus(200).withBody(payload)));
    server.stubFor(
        put(urlEqualTo("/v1/files/upload/docs/my%20file.txt"))
            .withHeader("Content-Type", equalTo("text/plain"))
            .willReturn(aResponse().withBody(envelope("{\"file_id\":\"f2\"}"))));
    server.stubFor(delete(urlPathEqualTo("/v1/files/f1")).willReturn(aResponse().withStatus(204)));
    server.stubFor(
        post(urlPathEqualTo("/v1/files/uploads"))
            .withRequestBody(equalToJson("{\"path\":\"big.bin\"}"))
            .willReturn(aResponse().withBody(envelope("{\"upload_id\":\"u1\"}"))));
    server.stubFor(
        put(urlPathEqualTo("/v1/files/uploads/u1/1"))
            .willReturn(aResponse().withBody(envelope("{\"etag\":\"e1\"}"))));
    server.stubFor(
        post(urlPathEqualTo("/v1/files/uploads/u1/complete"))
            .willReturn(aResponse().withBody(envelope("{\"file_id\":\"f3\"}"))));
    server.stubFor(
        delete(urlPathEqualTo("/v1/files/uploads/u1")).willReturn(aResponse().withStatus(204)));

    HotClient c = client();
    assertEquals(List.of(Map.of("file_id", "f1")), c.files().list(Map.of("prefix", "docs/")).get("data"));
    assertEquals("f1", c.files().get("f1").get("file_id"));
    try (InputStream body = c.files().download("f1").body()) {
      assertArrayEquals(payload, body.readAllBytes());
    }
    assertEquals("f2", c.files().upload("/docs/my file.txt", payload, "text/plain").get("file_id"));
    c.files().delete("f1");
    assertEquals("u1", c.files().initiateUpload(Map.of("path", "big.bin")).get("upload_id"));
    assertEquals("e1", c.files().uploadPart("u1", 1, payload).get("etag"));
    assertEquals("f3", c.files().completeUpload("u1").get("file_id"));
    c.files().abortUpload("u1");
  }

  @Test
  void buildsResourceCoversListGetDeployedLiveUploadDownloadDeploy() throws Exception {
    byte[] archive = {1, 2, 3, 4};

    server.stubFor(
        get(urlPathEqualTo("/v1/builds"))
            .willReturn(aResponse().withBody("{\"data\":[{\"build_id\":\"b1\"}],\"meta\":{}}")));
    server.stubFor(
        get(urlPathEqualTo("/v1/projects/proj/builds"))
            .willReturn(aResponse().withBody("{\"data\":[{\"build_id\":\"b1\"}],\"meta\":{}}")));
    server.stubFor(
        get(urlPathEqualTo("/v1/projects/proj/builds/b1"))
            .willReturn(aResponse().withBody(envelope("{\"build_id\":\"b1\"}"))));
    server.stubFor(
        get(urlPathEqualTo("/v1/projects/proj/builds/deployed"))
            .willReturn(aResponse().withBody(envelope("{\"build_id\":\"b1\"}"))));
    server.stubFor(
        get(urlPathEqualTo("/v1/projects/proj/builds/live"))
            .willReturn(aResponse().withBody(envelope("{\"build_id\":\"b1\"}"))));
    server.stubFor(
        post(urlPathEqualTo("/v1/projects/proj/builds"))
            .withHeader("Content-Type", containing("multipart/form-data; boundary="))
            .willReturn(aResponse().withBody(envelope("{\"build_id\":\"b2\"}"))));
    server.stubFor(
        get(urlPathEqualTo("/v1/projects/proj/builds/b2/download"))
            .willReturn(aResponse().withStatus(200).withBody(archive)));
    server.stubFor(
        post(urlPathEqualTo("/v1/projects/proj/builds/b2/deploy"))
            .willReturn(aResponse().withBody(envelope("{\"status\":\"deployed\"}"))));

    HotClient c = client();
    assertEquals(List.of(Map.of("build_id", "b1")), c.builds().list().get("data"));
    assertEquals(List.of(Map.of("build_id", "b1")), c.builds().listForProject("proj").get("data"));
    assertEquals("b1", c.builds().get("proj", "b1").get("build_id"));
    assertEquals("b1", c.builds().deployed("proj").get("build_id"));
    assertEquals("b1", c.builds().live("proj").get("build_id"));

    BuildUpload upload =
        new BuildUpload(archive, "sha256-xyz").filename("app\r\nBad: 1.tar.gz").buildId("b2");
    assertEquals("b2", c.builds().upload("proj", upload).get("build_id"));
    List<LoggedRequest> uploads =
        server.findAll(
            com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor(
                urlPathEqualTo("/v1/projects/proj/builds")));
    assertEquals(1, uploads.size());
    String multipartBody = uploads.get(0).getBodyAsString();
    assertTrue(multipartBody.contains("name=\"hash\""));
    assertTrue(multipartBody.contains("sha256-xyz"));
    assertTrue(multipartBody.contains("name=\"build_id\""));
    assertTrue(multipartBody.contains("filename=\"appBad: 1.tar.gz\""));
    assertFalse(multipartBody.contains("\r\nBad:"));

    HttpResponse<InputStream> dl = c.builds().download("proj", "b2");
    try (InputStream body = dl.body()) {
      assertArrayEquals(archive, body.readAllBytes());
    }
    assertEquals("deployed", c.builds().deploy("proj", "b2").get("status"));
  }

  @Test
  void domainsSessionsAndServiceKeysResourcesWorkEndToEnd() {
    // Domains
    server.stubFor(
        post(urlPathEqualTo("/v1/domains"))
            .willReturn(aResponse().withBody(envelope("{\"domain_id\":\"d1\"}"))));
    server.stubFor(
        get(urlPathEqualTo("/v1/domains"))
            .willReturn(aResponse().withBody("{\"data\":[{\"domain_id\":\"d1\"}],\"meta\":{}}")));
    server.stubFor(
        get(urlPathEqualTo("/v1/domains/d1"))
            .willReturn(aResponse().withBody(envelope("{\"domain_id\":\"d1\"}"))));
    server.stubFor(
        post(urlPathEqualTo("/v1/domains/d1/verify"))
            .willReturn(aResponse().withBody(envelope("{\"verified\":true}"))));
    server.stubFor(
        delete(urlPathEqualTo("/v1/domains/d1")).willReturn(aResponse().withStatus(204)));

    // Sessions
    server.stubFor(
        post(urlPathEqualTo("/v1/sessions"))
            .willReturn(aResponse().withBody(envelope("{\"session_id\":\"ses1\"}"))));
    server.stubFor(
        get(urlPathEqualTo("/v1/sessions"))
            .willReturn(aResponse().withBody("{\"data\":[{\"session_id\":\"ses1\"}],\"meta\":{}}")));
    server.stubFor(
        delete(urlPathEqualTo("/v1/sessions/ses1")).willReturn(aResponse().withStatus(204)));
    server.stubFor(
        delete(urlPathEqualTo("/v1/sessions"))
            .willReturn(aResponse().withBody(envelope("{\"revoked\":1}"))));

    // Service keys
    server.stubFor(
        post(urlPathEqualTo("/v1/service-keys"))
            .willReturn(aResponse().withBody(envelope("{\"service_key_id\":\"sk1\"}"))));
    server.stubFor(
        get(urlPathEqualTo("/v1/service-keys"))
            .willReturn(aResponse().withBody("{\"data\":[{\"service_key_id\":\"sk1\"}],\"meta\":{}}")));
    server.stubFor(
        get(urlPathEqualTo("/v1/service-keys/sk1"))
            .willReturn(aResponse().withBody(envelope("{\"service_key_id\":\"sk1\"}"))));
    server.stubFor(
        patch(urlPathEqualTo("/v1/service-keys/sk1"))
            .willReturn(aResponse().withBody(envelope("{\"description\":\"updated\"}"))));
    server.stubFor(
        delete(urlPathEqualTo("/v1/service-keys/sk1")).willReturn(aResponse().withStatus(204)));
    server.stubFor(
        delete(urlPathEqualTo("/v1/service-keys"))
            .willReturn(aResponse().withBody(envelope("{\"revoked\":2}"))));

    HotClient c = client();
    assertEquals("d1", c.domains().create(Map.of("domain", "app.example.com")).get("domain_id"));
    assertEquals(List.of(Map.of("domain_id", "d1")), c.domains().list().get("data"));
    assertEquals("d1", c.domains().get("d1").get("domain_id"));
    assertEquals(true, c.domains().verify("d1").get("verified"));
    c.domains().delete("d1");

    assertEquals("ses1", c.sessions().create(Map.of("permissions", Map.of())).get("session_id"));
    assertEquals(List.of(Map.of("session_id", "ses1")), c.sessions().list().get("data"));
    c.sessions().revoke("ses1");
    assertEquals(1, c.sessions().revokeAll().get("revoked"));

    assertEquals("sk1", c.serviceKeys().create(Map.of("description", "ci")).get("service_key_id"));
    assertEquals(List.of(Map.of("service_key_id", "sk1")), c.serviceKeys().list().get("data"));
    assertEquals("sk1", c.serviceKeys().get("sk1").get("service_key_id"));
    assertEquals(
        "updated", c.serviceKeys().update("sk1", Map.of("description", "updated")).get("description"));
    c.serviceKeys().revoke("sk1");
    assertEquals(2, c.serviceKeys().revokeAll().get("revoked"));
  }

  @Test
  void delete204EndpointsRetryOn503() {
    server.stubFor(
        delete(urlPathEqualTo("/v1/projects/p1"))
            .inScenario("del-retry")
            .whenScenarioStateIs(Scenario.STARTED)
            .willSetStateTo("ok")
            .willReturn(
                aResponse()
                    .withStatus(503)
                    .withBody("{\"error\":{\"code\":\"unavailable\",\"message\":\"busy\"}}")));
    server.stubFor(
        delete(urlPathEqualTo("/v1/projects/p1"))
            .inScenario("del-retry")
            .whenScenarioStateIs("ok")
            .willReturn(aResponse().withStatus(204)));

    server.stubFor(
        delete(urlPathEqualTo("/v1/projects/p1/context/k1"))
            .willReturn(aResponse().withStatus(204)));

    HotClient c = client();
    c.projects().delete("p1");
    c.context().delete("p1", "k1");

    server.verify(2, deleteRequestedFor(urlPathEqualTo("/v1/projects/p1")));
    server.verify(1, deleteRequestedFor(urlPathEqualTo("/v1/projects/p1/context/k1")));
  }
}
