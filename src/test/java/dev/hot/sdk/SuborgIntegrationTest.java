package dev.hot.sdk;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.function.Executable;

/** Live sub-org isolation tests. Run integration/suborg_fixture.py with a disposable database. */
@EnabledIfEnvironmentVariable(named = "HOT_TEST_SUBORG_FIXTURE", matches = ".+")
class SuborgIntegrationTest {
  private Map<String, Object> fixture() throws Exception {
    return new ObjectMapper()
        .readValue(
            System.getenv("HOT_TEST_SUBORG_FIXTURE"),
            new TypeReference<>() {});
  }

  private HotClient runtime(String token) {
    return HotClient.builder(token)
        .baseUrl(System.getenv("HOT_TEST_BASE_URL"))
        .timeout(Duration.ofSeconds(10))
        .build();
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> object(Object value) {
    return (Map<String, Object>) value;
  }

  private void denied(Executable call, int... statuses) {
    HotApiException error = assertThrows(HotApiException.class, call);
    for (int status : statuses) {
      if (error.statusCode() == status) return;
    }
    fail("Unexpected denial status: " + error.statusCode());
  }

  @Test
  void provisioningAndIsolation() throws Exception {
    Map<String, Object> conf = fixture();
    HotManagementClient management =
        HotManagementClient.builder((String) conf.get("management_token"))
            .baseUrl(System.getenv("HOT_TEST_BASE_URL"))
            .timeout(Duration.ofSeconds(10))
            .build();
    List<String> children = new ArrayList<>(), envs = new ArrayList<>();
    List<HotClient> clients = new ArrayList<>();
    try {
      for (int i = 0; i < 2; i++) {
        String id = UUID.randomUUID().toString();
        Map<String, Object> body =
            Map.of(
                "name",
                "SDK Customer",
                "slug",
                "sdk-" + id,
                "external_id",
                id,
                "idempotency_key",
                id);
        Map<String, Object> child = management.createChild(body);
        String childId = (String) object(child.get("org")).get("org_id");
        children.add(childId);
        Map<String, Object> replay = management.createChild(body);
        assertEquals(false, replay.get("created"));
        assertEquals(childId, object(replay.get("org")).get("org_id"));
        Map<String, Object> env =
            management.createEnvironment(childId, Map.of("name", "production"));
        assertEquals(env, management.createEnvironment(childId, Map.of("name", "production")));
        String envId = (String) env.get("env_id");
        envs.add(envId);
        Map<String, Object> key =
            management.createApiKey(
                childId,
                envId,
                Map.of("description", "sdk", "permissions", Map.of("*:*", List.of("*"))));
        HotClient client = runtime((String) key.get("token"));
        clients.add(client);
        assertEquals(childId, client.env().get().get("org_id"));
        Map<String, Object> usage = client.org().usage();
        assertEquals(envId, usage.get("env_id"));
        assertEquals("environment", usage.get("usage_scope"));
        assertEquals("billing_account", usage.get("limits_scope"));
      }
      HotClient a = clients.get(0), b = clients.get(1);
      Map<String, Object> pa = a.projects().create(Map.of("name", "shared-name")),
          pb = b.projects().create(Map.of("name", "shared-name"));
      assertNotEquals(pa.get("project_id"), pb.get("project_id"));
      assertEquals(pa.get("project_id"), a.projects().get("shared-name").get("project_id"));
      assertEquals(pb.get("project_id"), b.projects().get("shared-name").get("project_id"));
      denied(() -> b.projects().get((String) pa.get("project_id")), 404);
      denied(
          () ->
              runtime((String) conf.get("root_token"))
                  .projects()
                  .get((String) pa.get("project_id")),
          404);
      Map<String, Object> eventBody =
          Map.of("event_type", "sdk:isolation", "event_data", Map.of("owner", "a"));
      Map<String, Object> event = a.events().publish(eventBody);
      assertEquals(
          Map.of("owner", "a"), a.events().get((String) event.get("event_id")).get("event_data"));
      denied(() -> b.events().get((String) event.get("event_id")), 404);
      denied(
          () -> {
            try (StreamEvents stream = b.streams().subscribe((String) event.get("stream_id"))) {
              stream.hasNext();
            }
          },
          403,
          404);
      denied(
          () ->
              HotManagementClient.builder((String) conf.get("root_token"))
                  .baseUrl(System.getenv("HOT_TEST_BASE_URL"))
                  .build()
                  .listChildren(null),
          401);
      denied(() -> runtime((String) conf.get("management_token")).env().get(), 401);
      assertTrue(((Number) management.listChildren(null).get("total")).intValue() >= 2);
      assertEquals(
          conf.get("root_org_id"),
          AsyncHotManagementClient.wrap(management).usage().get().get("root_org_id"));
      Map<String, Object> restricted =
          management.createApiKey(
              children.get(0),
              envs.get(0),
              Map.of(
                  "description",
                  "event-only",
                  "permissions",
                  Map.of("event:sdk:*", List.of("create", "read"))));
      HotClient limited = runtime((String) restricted.get("token"));
      limited.events().publish(eventBody);
      denied(() -> limited.projects().list(), 403);
      Map<String, Object> permissions =
          Map.of("event:sdk:*", List.of("create", "read"), "stream:*", List.of("read"));
      Map<String, Object> service =
          a.serviceKeys().create(Map.of("name", "sdk-service", "permissions", permissions));
      Map<String, Object> session = a.sessions().create(Map.of("permissions", permissions));
      Map<String, Object> other = b.events().publish(eventBody);
      for (Map<String, Object> credential : List.of(service, session)) {
        HotClient client = runtime((String) credential.get("token"));
        client.events().publish(eventBody);
        denied(() -> client.events().get((String) other.get("event_id")), 403, 404);
      }
      a.serviceKeys().revoke((String) service.get("service_key_id"));
      a.sessions().revoke((String) session.get("session_id"));
      for (Map<String, Object> credential : List.of(service, session)) {
        denied(() -> runtime((String) credential.get("token")).events().publish(eventBody), 401);
      }
      management.setChildState(children.get(0), "suspended");
      denied(() -> a.env().get(), 401, 403);
      b.env().get();
      management.setChildState(children.get(0), "active");
      a.env().get();
    } finally {
      for (String child : children) management.setChildState(child, "archived");
    }
  }

  @Test
  void issuerRevocationAndNarrowing() throws Exception {
    Map<String, Object> conf = fixture();
    Map<String, Object> event = Map.of("event_type", "sdk:isolation", "event_data", Map.of());
    for (Object token : (List<?>) conf.get("revoked")) {
      denied(() -> runtime((String) token).events().publish(event), 401);
    }
    for (Object token : (List<?>) conf.get("narrowed")) {
      HotClient client = runtime((String) token);
      client.events().publish(event);
      denied(() -> client.projects().list(), 403);
      denied(
          () ->
              client
                  .events()
                  .publish(Map.of("event_type", "other:blocked", "event_data", Map.of())),
          403);
    }
  }
}
