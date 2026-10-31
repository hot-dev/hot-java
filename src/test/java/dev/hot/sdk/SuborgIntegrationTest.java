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

/**
 * Live org API and sub-org isolation tests. Run integration/suborg_fixture.py, which seeds an org
 * API key in a disposable database.
 */
@EnabledIfEnvironmentVariable(named = "HOT_TEST_SUBORG_FIXTURE", matches = ".+")
class SuborgIntegrationTest {
  private Map<String, Object> fixture() throws Exception {
    return new ObjectMapper()
        .readValue(System.getenv("HOT_TEST_SUBORG_FIXTURE"), new TypeReference<>() {});
  }

  private HotClient client(String token) {
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

  private static String slug() {
    return "sdk-" + UUID.randomUUID().toString().replace("-", "").substring(0, 24);
  }

  @Test
  void provisioningAndIsolation() throws Exception {
    Map<String, Object> conf = fixture();
    HotClient org = client((String) conf.get("org_token"));
    String rootOrg = (String) conf.get("root_org_id");
    List<String> children = new ArrayList<>(), envs = new ArrayList<>(), keyIds = new ArrayList<>();
    List<HotClient> clients = new ArrayList<>();
    try {
      for (int i = 0; i < 2; i++) {
        String slug = slug();
        Map<String, Object> body =
            Map.of("name", "SDK Customer", "slug", slug, "external_id", slug, "idempotency_key", slug);
        Map<String, Object> child = org.orgs().create(body);
        assertEquals(true, child.get("created"));
        String childId = (String) object(child.get("org")).get("org_id");
        children.add(childId);
        Map<String, Object> replay = org.orgs().create(body);
        assertEquals(false, replay.get("created"));
        assertEquals(childId, object(replay.get("org")).get("org_id"));
        assertEquals(slug, org.orgs().get(childId).get("slug"));
        Map<String, Object> env = org.orgs().createEnv(childId, Map.of("name", "production"));
        assertEquals(
            env.get("env_id"),
            org.orgs().createEnv(childId, Map.of("name", "production")).get("env_id"));
        String envId = (String) env.get("env_id");
        envs.add(envId);
        assertEquals(envId, org.orgs().getEnv(childId, envId).get("env_id"));
        assertEquals(2, ((List<?>) org.orgs().listEnvs(childId).get("data")).size());
        Map<String, Object> key =
            org.orgs()
                .createEnvApiKey(
                    childId,
                    envId,
                    Map.of("description", "sdk", "permissions", Map.of("*:*", List.of("*"))));
        assertEquals("env", key.get("scope"));
        keyIds.add((String) key.get("api_key_id"));
        HotClient client = client((String) key.get("token"));
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
              client((String) conf.get("root_token"))
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

      // Each key scope gets a typed error on the other scope's routes.
      HotKeyScopeException runtimeWithOrgKey =
          assertThrows(HotKeyScopeException.class, () -> org.env().get());
      assertEquals("env", runtimeWithOrgKey.requiredScope());
      HotKeyScopeException orgWithEnvKey =
          assertThrows(HotKeyScopeException.class, () -> a.orgs().list());
      assertEquals("org", orgWithEnvKey.requiredScope());
      assertThrows(
          HotKeyScopeException.class,
          () -> client((String) conf.get("root_token")).apiKeys().get(keyIds.get(0)));

      Map<String, Object> listed = org.orgs().list(Map.of("limit", "500"));
      assertTrue(((Number) object(listed.get("pagination")).get("total")).intValue() >= 2);
      Map<String, Object> usage = AsyncHotClient.wrap(org).orgs().usage(rootOrg).get();
      assertEquals(rootOrg, usage.get("org_id"));
      assertEquals("account", usage.get("usage_scope"));

      // Org-scoped project and context routes inside a child environment.
      OrgEnv scoped = org.orgs().env(children.get(1), envs.get(1));
      Map<String, Object> project = scoped.projects().create(Map.of("name", "provisioned"));
      assertEquals(project.get("project_id"), b.projects().get("provisioned").get("project_id"));
      scoped.context().create("provisioned", Map.of("key", "callback.url", "value", "https://x"));
      assertEquals(
          "callback.url",
          object(((List<?>) b.context().list("provisioned").get("data")).get(0)).get("key"));
      assertTrue(
          ((List<?>) org.orgs().listEnvApiKeys(children.get(0), envs.get(0)).get("data"))
              .stream()
              .anyMatch(k -> keyIds.get(0).equals(object(k).get("api_key_id"))));

      Map<String, Object> restricted =
          org.orgs()
              .createEnvApiKey(
                  children.get(0),
                  envs.get(0),
                  Map.of(
                      "description",
                      "event-only",
                      "permissions",
                      Map.of("event:sdk:*", List.of("create", "read"))));
      HotClient limited = client((String) restricted.get("token"));
      limited.events().publish(eventBody);
      denied(() -> limited.projects().list(), 403);

      // Rotation with no overlap retires the old secret; revocation stops the key.
      String restrictedId = (String) restricted.get("api_key_id");
      assertEquals("env", org.apiKeys().get(restrictedId).get("scope"));
      Map<String, Object> rotated = org.apiKeys().rotate(restrictedId, Map.of("overlap_seconds", 0));
      client((String) rotated.get("token")).events().publish(eventBody);
      denied(() -> limited.events().publish(eventBody), 401);
      org.apiKeys().revoke(restrictedId);
      denied(() -> client((String) rotated.get("token")).events().publish(eventBody), 401);

      Map<String, Object> permissions =
          Map.of("event:sdk:*", List.of("create", "read"), "stream:*", List.of("read"));
      Map<String, Object> service =
          a.serviceKeys().create(Map.of("name", "sdk-service", "permissions", permissions));
      Map<String, Object> session = a.sessions().create(Map.of("permissions", permissions));
      Map<String, Object> other = b.events().publish(eventBody);
      for (Map<String, Object> credential : List.of(service, session)) {
        HotClient client = client((String) credential.get("token"));
        client.events().publish(eventBody);
        denied(() -> client.events().get((String) other.get("event_id")), 403, 404);
      }
      a.serviceKeys().revoke((String) service.get("service_key_id"));
      a.sessions().revoke((String) session.get("session_id"));
      for (Map<String, Object> credential : List.of(service, session)) {
        denied(() -> client((String) credential.get("token")).events().publish(eventBody), 401);
      }
      assertEquals(
          "suspended",
          org.orgs().update(children.get(0), Map.of("state", "suspended")).get("lifecycle_state"));
      denied(() -> a.env().get(), 401, 403);
      b.env().get();
      org.orgs().update(children.get(0), Map.of("state", "active"));
      a.env().get();
    } finally {
      for (String child : children) org.orgs().update(child, Map.of("state", "archived"));
    }
  }

  @Test
  void issuerRevocationAndNarrowing() throws Exception {
    Map<String, Object> conf = fixture();
    Map<String, Object> event = Map.of("event_type", "sdk:isolation", "event_data", Map.of());
    for (Object token : (List<?>) conf.get("revoked")) {
      denied(() -> client((String) token).events().publish(event), 401);
    }
    for (Object token : (List<?>) conf.get("narrowed")) {
      HotClient client = client((String) token);
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
