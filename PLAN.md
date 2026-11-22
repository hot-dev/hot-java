# hot-java — Architecture & Implementation Plan

Official Java SDK for the Hot Dev API. Follows the shared Hot SDK conventions
across JavaScript, Python, Go, Rust, and Java.

## Package identity

- Maven coordinates: **`dev.hot:hot-sdk`** (`Automatic-Module-Name: dev.hot.sdk`).
- Java package: `dev.hot.sdk` (internal implementation under `dev.hot.sdk.internal`).
- License: Apache-2.0.
- Java baseline: **17** (`--release 17`, CI tests on JDK 17, 21, and 25 toolchains).
- Versioning: lockstep with the other SDKs (`gradle.properties` `VERSION_NAME`); tags `v<version>`.

## Tech choices

- **Build**: Gradle (Kotlin DSL) with the wrapper checked in.
- **HTTP**: `java.net.http.HttpClient` (JDK built-in) — keeps the runtime
  footprint to Jackson only. Default `HotClient` instances share a single lazy
  `HttpClient` (`connectTimeout = 10s`, `Redirect.NEVER`), and callers can inject
  their own `HttpClient` (must keep `Redirect.NEVER`).
- **JSON**: Jackson (`com.fasterxml.jackson.core:jackson-databind:2.23.0`) as a
  direct runtime dependency (no exported `jackson-bom` platform). Optional
  `useBigDecimalForFloats(true)` preserves arbitrary-precision Hot `Dec` values.
- **Typing policy**: payloads are `Map<String, Object>` in the Hot API wire
  format (`event_type`, `stream_id`); the SDK never transforms user-owned
  payloads (`event_data`). SDK-only options use Java-style names (`baseUrl`,
  `timeout`).
- **Sync + async**: `HotClient` (blocking, `AutoCloseable`) and `AsyncHotClient`
  (`CompletableFuture`-returning, backed by a bounded daemon `ThreadPoolExecutor`
  scaling down to zero when idle). SSE subscriptions return `StreamEvents`
  (`Iterator<Map<String, Object>>` + `AutoCloseable`) synchronously and drive a
  `Consumer<Map<String, Object>>` asynchronously with cancellation support.
- **Errors**: unchecked `HotApiException` (and `HotKeyScopeException` subclass
  for `403 key_scope_mismatch`), `HotTransportException`, `HotStreamException`,
  `HotRunException`, `HotTaskException`, and `HotTimeoutException`.
- **Config**: `HotClient.builder(token).baseUrl(...).timeout(...).httpClient(...).useBigDecimalForFloats(...).build()`.

## Layout

```
dev/hot/sdk/
  HotClient.java, AsyncHotClient.java   — builders, status(), resource accessors,
                                          request()/requestRaw() escape hatches
  *Resource.java, OrgEnv.java           — 16 resource namespaces (Events, Streams,
                                          Runs, Tasks, Files, Blobs, Projects,
                                          Builds, Context, Domains, Sessions,
                                          ServiceKeys, Org, Env, Orgs, ApiKeys)
  StreamEvents.java                     — SSE event iterator + wire helpers
  Hot*Exception.java                    — structured unchecked exception hierarchy
  *Options.java, BuildUpload.java       — typed option & upload builders
  internal/Transport.java               — auth, retries, path validation, raw & SSE
  internal/SseEvents.java               — WHATWG UTF-8 SSE parser + idle watchdog
  internal/ReconnectingEvents.java      — subscribeWithEvent reconnect + dedupe loop
  internal/RunWaiter.java               — durable run/task/call waiters + deadlines
  internal/RunCorrelation.java          — origin_run_id & retry_run_id correlation
  internal/Backoff.java                 — shared jittered backoff & permanent-error policy
  internal/Json.java, Multipart.java, Responses.java
```

## Testing

- **Unit tests**: JUnit 6 + WireMock (`wiremock-jetty12`), covering transport
  retries, path validation, SSE framing, reconnect/rollover policy, retry
  correlation, org API scope enforcement, async cancellation, and all resources.
- **OpenAPI coverage**: `OpenApiCoverageTest` checks every operation in
  `src/test/resources/openapi-operations.json` (85/85 operations mapped to live
  SDK methods, including `GET /status`).
- **Live integration tests**: `integration/run.sh` against `hot dev`
  (`IntegrationTest`) and `integration/suborg_fixture.py` against a pinned Hot
  server with PostgreSQL RLS (`SuborgIntegrationTest`).
- **CI**: `./gradlew build javadoc` on JDK 17, 21, and 25, plus `integration`
  and `org-api-isolation` jobs in `.github/workflows/ci.yml`.

## Publishing (Maven Central)

Publishes to Maven Central Portal via `com.vanniktech.maven.publish`
(`publishAndReleaseToMavenCentral`) on `v<version>` tags matching
`gradle.properties` (`.github/workflows/release.yml`), running
`./gradlew build javadoc` before publishing.
