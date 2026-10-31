package dev.hot.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Every OpenAPI operation must map to an SDK method or carry an explicit ignore ("" value).
 * Regenerate the fixture from a local `hot dev` server's /openapi.json (see hot-js
 * scripts/update-openapi-operations.mjs for the format).
 */
class OpenApiCoverageTest {

  private static final Map<String, String> COVERAGE = buildCoverage();

  private static Map<String, String> buildCoverage() {
    Map<String, String> coverage = new LinkedHashMap<>();
    // Org API (org API keys).
    coverage.put("GET /v1/orgs", "orgs.list");
    coverage.put("POST /v1/orgs", "orgs.create");
    coverage.put("GET /v1/orgs/{org_id}", "orgs.get");
    coverage.put("PATCH /v1/orgs/{org_id}", "orgs.update");
    coverage.put("GET /v1/orgs/{org_id}/usage", "orgs.usage");
    coverage.put("DELETE /v1/orgs/{org_id}/calls", "orgs.purgeCalls");
    coverage.put("GET /v1/orgs/{org_id}/envs", "orgs.listEnvs");
    coverage.put("POST /v1/orgs/{org_id}/envs", "orgs.createEnv");
    coverage.put("GET /v1/orgs/{org_id}/envs/{env_id}", "orgs.getEnv");
    coverage.put("GET /v1/orgs/{org_id}/api-keys", "orgs.listApiKeys");
    coverage.put("POST /v1/orgs/{org_id}/api-keys", "orgs.createApiKey");
    coverage.put("GET /v1/orgs/{org_id}/envs/{env_id}/api-keys", "orgs.listEnvApiKeys");
    coverage.put("POST /v1/orgs/{org_id}/envs/{env_id}/api-keys", "orgs.createEnvApiKey");
    // The spec documents only the env projects and builds list routes; the other forwarded
    // project, build and context routes are not in it.
    coverage.put("GET /v1/orgs/{org_id}/envs/{env_id}/projects", "orgEnv.projects.list");
    coverage.put("GET /v1/orgs/{org_id}/envs/{env_id}/builds", "orgEnv.builds.list");
    coverage.put("GET /v1/api-keys/{key_id}", "apiKeys.get");
    coverage.put("PATCH /v1/api-keys/{key_id}", "apiKeys.update");
    coverage.put("DELETE /v1/api-keys/{key_id}", "apiKeys.revoke");
    coverage.put("POST /v1/api-keys/{key_id}/rotate", "apiKeys.rotate");
    coverage.put("DELETE /v1/env/calls", "env.purgeCalls");
    coverage.put("DELETE /v1/runs/{run_id}/calls", "runs.purgeCalls");
    coverage.put("DELETE /v1/streams/{stream_id}/calls", "streams.purgeCalls");
    coverage.put("GET /status", ""); // public health endpoint
    coverage.put("GET /v1/projects", "projects.list");
    coverage.put("POST /v1/projects", "projects.create");
    coverage.put("GET /v1/projects/{project_id_or_slug}", "projects.get");
    coverage.put("PATCH /v1/projects/{project_id_or_slug}", "projects.update");
    coverage.put("DELETE /v1/projects/{project_id_or_slug}", "projects.delete");
    coverage.put("POST /v1/projects/{project_id_or_slug}/activate", "projects.activate");
    coverage.put("POST /v1/projects/{project_id_or_slug}/deactivate", "projects.deactivate");
    coverage.put("GET /v1/projects/{project_id_or_slug}/event-handlers", "projects.eventHandlers");
    coverage.put("GET /v1/projects/{project_id_or_slug}/schedules", "projects.schedules");
    coverage.put("GET /v1/builds", "builds.list");
    coverage.put("GET /v1/projects/{project_id_or_slug}/builds", "builds.listForProject");
    coverage.put("POST /v1/projects/{project_id_or_slug}/builds", "builds.upload");
    coverage.put("GET /v1/projects/{project_id_or_slug}/builds/deployed", "builds.deployed");
    coverage.put("GET /v1/projects/{project_id_or_slug}/builds/live", "builds.live");
    coverage.put("GET /v1/projects/{project_id_or_slug}/builds/{build_id}", "builds.get");
    coverage.put(
        "GET /v1/projects/{project_id_or_slug}/builds/{build_id}/download", "builds.download");
    coverage.put(
        "POST /v1/projects/{project_id_or_slug}/builds/{build_id}/deploy", "builds.deploy");
    coverage.put("GET /v1/projects/{project_id_or_slug}/context", "context.list");
    coverage.put("POST /v1/projects/{project_id_or_slug}/context", "context.create");
    coverage.put("PUT /v1/projects/{project_id_or_slug}/context/{key}", "context.update");
    coverage.put("DELETE /v1/projects/{project_id_or_slug}/context/{key}", "context.delete");
    coverage.put("GET /v1/runs", "runs.list");
    coverage.put("GET /v1/runs/stats", "runs.stats");
    coverage.put("GET /v1/runs/{run_id}", "runs.get");
    coverage.put("GET /v1/runs/{run_id}/subscribe", "runs.subscribe");
    coverage.put("GET /v1/tasks/{task_id}", "tasks.get");
    coverage.put("GET /v1/tasks/{task_id}/subscribe", "tasks.subscribe");
    coverage.put("POST /v1/events", "events.publish");
    coverage.put("GET /v1/events", "events.list");
    coverage.put("GET /v1/events/{event_id}", "events.get");
    coverage.put("GET /v1/events/{event_id}/runs", "events.getRuns");
    coverage.put("POST /v1/sessions", "sessions.create");
    coverage.put("GET /v1/sessions", "sessions.list");
    coverage.put("DELETE /v1/sessions", "sessions.revokeAll");
    coverage.put("DELETE /v1/sessions/{session_id}", "sessions.revoke");
    coverage.put("POST /v1/service-keys", "serviceKeys.create");
    coverage.put("GET /v1/service-keys", "serviceKeys.list");
    coverage.put("DELETE /v1/service-keys", "serviceKeys.revokeAll");
    coverage.put("GET /v1/service-keys/{service_key_id}", "serviceKeys.get");
    coverage.put("PATCH /v1/service-keys/{service_key_id}", "serviceKeys.update");
    coverage.put("DELETE /v1/service-keys/{service_key_id}", "serviceKeys.revoke");
    coverage.put("POST /v1/domains", "domains.create");
    coverage.put("GET /v1/domains", "domains.list");
    coverage.put("GET /v1/domains/{domain_id}", "domains.get");
    coverage.put("DELETE /v1/domains/{domain_id}", "domains.delete");
    coverage.put("POST /v1/domains/{domain_id}/verify", "domains.verify");
    coverage.put("GET /v1/env", "env.get");
    coverage.put("GET /v1/env/subscribe", "env.subscribe");
    coverage.put("GET /v1/org/usage", "org.usage");
    coverage.put("GET /v1/streams/{stream_id}/subscribe", "streams.subscribe");
    coverage.put("POST /v1/streams/{stream_id}/subscribe", "streams.subscribePost");
    coverage.put("POST /v1/streams/subscribe-with-event", "streams.subscribeWithEvent");
    coverage.put("GET /v1/files", "files.list");
    coverage.put("GET /v1/files/{file_id}", "files.get");
    coverage.put("DELETE /v1/files/{file_id}", "files.delete");
    coverage.put("GET /v1/files/{file_id}/download", "files.download");
    coverage.put("PUT /v1/files/upload/{path}", "files.upload");
    coverage.put("POST /v1/files/uploads", "files.initiateUpload");
    coverage.put("PUT /v1/files/uploads/{upload_id}/{part_number}", "files.uploadPart");
    coverage.put("POST /v1/files/uploads/{upload_id}/complete", "files.completeUpload");
    coverage.put("DELETE /v1/files/uploads/{upload_id}", "files.abortUpload");
    coverage.put("GET /v1/blobs/{blob_ref_id}/download", "blobs.download");
    return coverage;
  }

  @Test
  void openApiOperationsHaveSdkCoverageOrExplicitIgnore() throws Exception {
    Set<String> operations;
    try (InputStream fixture = getClass().getResourceAsStream("/openapi-operations.json")) {
      List<Map<String, String>> parsed =
          new ObjectMapper().readValue(fixture, new TypeReference<>() {});
      operations =
          parsed.stream()
              .map(operation -> operation.get("method") + " " + operation.get("path"))
              .collect(Collectors.toSet());
    }

    assertEquals(operations, COVERAGE.keySet());
  }

  @Test
  void coveredOperationsPointToExistingSdkMethods() throws Exception {
    HotClient client = HotClient.builder("test-token").build();

    for (Map.Entry<String, String> entry : COVERAGE.entrySet()) {
      if (entry.getValue().isEmpty()) {
        continue;
      }
      String[] parts = entry.getValue().split("\\.");
      Object resource;
      if (parts[0].equals("orgEnv")) {
        resource = client.orgs().env("org", "env");
      } else {
        Method accessor = HotClient.class.getMethod(parts[0]);
        resource = accessor.invoke(client);
      }
      for (int i = 1; i < parts.length - 1; i++) {
        resource = resource.getClass().getMethod(parts[i]).invoke(resource);
      }
      String name = parts[parts.length - 1];
      boolean exists =
          Arrays.stream(resource.getClass().getMethods())
              .anyMatch(method -> method.getName().equals(name));
      assertTrue(exists, entry.getKey() + " -> client." + entry.getValue());
    }
  }
}
