package dev.hot.sdk;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.jupiter.api.Assertions.*;

import com.github.tomakehurst.wiremock.WireMockServer;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ManagementTest {
  @Test
  void managementContract() throws Exception {
    WireMockServer server = new WireMockServer(options().dynamicPort());
    server.start();
    try {
      for (String route :
          new String[] {
            "/v1/management/children",
            "/v1/management/children/child%2Fone/environments",
            "/v1/management/children/child%2Fone/environments/env%2Ftwo/api-keys"
          }) {
        server.stubFor(
            post(urlEqualTo(route))
                .withHeader("Authorization", equalTo("Bearer hm_test"))
                .withRequestBody(equalToJson("{\"name\":\"sdk\"}"))
                .willReturn(okJson("{\"data\":{\"marker\":\"response\"},\"meta\":{}}")));
      }
      server.stubFor(
          get(urlPathEqualTo("/v1/management/children"))
              .withQueryParam("limit", equalTo("7"))
              .withQueryParam("offset", equalTo("3"))
              .willReturn(okJson("{\"data\":{\"children\":[],\"total\":0}}")));
      server.stubFor(
          get(urlEqualTo("/v1/management/usage"))
              .willReturn(okJson("{\"data\":{\"root_org_id\":\"root\"}}")));
      server.stubFor(
          patch(urlEqualTo("/v1/management/children/child%2Fone/state"))
              .withRequestBody(equalToJson("{\"state\":\"suspended\"}"))
              .willReturn(aResponse().withStatus(204)));
      HotManagementClient client =
          HotManagementClient.builder("hm_test").baseUrl(server.baseUrl()).build();
      assertEquals(0, client.listChildren(Map.of("limit", "7", "offset", "3")).get("total"));
      assertEquals("response", client.createChild(Map.of("name", "sdk")).get("marker"));
      client.setChildState("child/one", "suspended");
      client.createEnvironment("child/one", Map.of("name", "sdk"));
      client.createApiKey("child/one", "env/two", Map.of("name", "sdk"));
      assertEquals(
          "root",
          AsyncHotManagementClient.wrap(client, Runnable::run).usage().get().get("root_org_id"));
      assertEquals(6, server.getAllServeEvents().size());
    } finally {
      server.stop();
    }
  }

  @Test
  void authorizationFailuresStopWaiters() {
    for (int status : new int[] {401, 403}) {
      WireMockServer server = new WireMockServer(options().dynamicPort());
      server.start();
      try {
        server.stubFor(
            any(anyUrl()).willReturn(aResponse().withStatus(status).withBody("revoked")));
        HotClient client = HotClient.builder("test").baseUrl(server.baseUrl()).build();
        assertEquals(
            status,
            assertThrows(HotApiException.class, () -> client.tasks().waitFor("t")).statusCode());
        assertEquals(
            status,
            assertThrows(HotApiException.class, () -> client.runs().waitFor("r")).statusCode());
        assertEquals(2, server.getAllServeEvents().size());
      } finally {
        server.stop();
      }
    }
  }

  @Test
  void authorizationFailureClosesReconnectingIterator() {
    for (int status : new int[] {401, 403}) {
      WireMockServer server = new WireMockServer(options().dynamicPort());
      server.start();
      try {
        server.stubFor(
            post(urlEqualTo("/v1/streams/subscribe-with-event"))
                .willReturn(
                    ClientTest.sse(
                        "{\"type\":\"event:published\",\"stream_id\":\"s\",\"event_id\":\"e\"}")));
        server.stubFor(
            get(urlEqualTo("/v1/streams/s/subscribe"))
                .willReturn(aResponse().withStatus(status).withBody("revoked")));
        HotClient client = HotClient.builder("test").baseUrl(server.baseUrl()).build();
        try (StreamEvents events = client.streams().subscribeWithEvent(Map.of())) {
          assertTrue(events.hasNext());
          events.next();
          assertEquals(status, assertThrows(HotApiException.class, events::hasNext).statusCode());
          assertFalse(events.hasNext());
        }
        assertEquals(2, server.getAllServeEvents().size());
      } finally {
        server.stop();
      }
    }
  }
}
