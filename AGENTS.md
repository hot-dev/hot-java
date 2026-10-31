# AGENTS.md - hot-java

This repo contains the Java SDK for the Hot Dev API.

## Package Shape

- Maven coordinates: `dev.hot:hot-sdk`; package `dev.hot.sdk`
- Java 17 baseline (toolchain builds on 21, `--release 17`)
- HTTP via `java.net.http.HttpClient`; Jackson is the only runtime dependency
- Payloads are `Map<String, Object>` in the Hot API wire format (`event_type`,
  `stream_id`); the SDK never transforms user-owned payloads such as
  `event_data`. SDK-only options use Java-style names (`baseUrl`, `timeout`).
- `HotClient` (sync) is canonical; `AsyncHotClient` is a thin
  CompletableFuture wrapper over it on a configurable executor (default: a
  dedicated daemon pool, never the common pool).
- Two key scopes: org API keys call `orgs()` / `apiKeys()` only; environment
  keys, sessions and service keys call everything else. `403
  key_scope_mismatch` becomes `HotKeyScopeException` with `requiredScope()`.
- Versioning follows the public API package version, in lockstep with
  `@hot-dev/sdk` (npm) and `hot-dev` (PyPI). Version lives in
  `gradle.properties` (`VERSION_NAME`).

## Commands

```bash
./gradlew build      # compile + test + jar
./gradlew test
./gradlew javadoc
```

## Layout

- `HotClient` / `AsyncHotClient` — builders, resource accessors, escape hatches
- `internal/Transport` — auth, JSON envelope, raw, and SSE requests
- `internal/SseEvents` / `internal/ReconnectingEvents` — SSE decode (with an
  idle timeout) + the subscribeWithEvent reconnect loop (dedupes replayed run
  events by run_id)
- `internal/Backoff` — the reconnect policy every waiter shares: stop on
  permanent 4xx, jittered backoff honouring Retry-After for transient errors,
  a consecutive-failure budget that stream:complete rollovers reset
- `internal/RunWaiter` / `internal/RunCorrelation` — durable waiters with an
  absolute deadline; tie runs by event_id or origin_run_id (retries, with
  orphans keyed by origin), buffer stream:data until the run ties, and hold a
  failure whose raised retry_attempt announces a retry for a 5 s grace (the
  API replays retried attempts lossily; same rule as hot-rust's `RunTracker`)
- one class per resource (mirroring hot-python's resources.py), plus
  `OrgsResource`, `OrgEnv` and `ApiKeysResource` for the org API
- `OpenApiCoverageTest` — every OpenAPI operation must map to an SDK method or
  carry an explicit ignore; fixture in `src/test/resources/`.

## CI and Releases

- CI runs on pushes and pull requests to `main`: `./gradlew build javadoc` on
  JDK 17, 21 and 25 toolchains, `integration/run.sh` against the released hot,
  and the org API isolation job, which builds hot at `integration/hot-server.ref`
  and runs `SuborgIntegrationTest` on restricted PostgreSQL. A weekly scheduled
  run builds hot `main` instead, to catch API drift.
- Maven Central releases run from tags named `v<version>` matching
  `gradle.properties` and go through the **Central Portal**
  (central.sonatype.com) via the `com.vanniktech.maven.publish` plugin
  (`publishAndReleaseToMavenCentral`). The legacy OSSRH/Nexus flow is dead —
  ignore docs that mention oss.sonatype.org staging.
- Required repo secrets:
  - `MAVEN_CENTRAL_USERNAME` / `MAVEN_CENTRAL_PASSWORD` — Central Portal user token
  - `SIGNING_KEY` — ASCII-armored GPG private key
  - `SIGNING_PASSWORD` — its passphrase
- One-time setup: Central Portal account, `dev.hot` namespace verified via DNS
  TXT on hot.dev, GPG public key pushed to keyserver.ubuntu.com.
- Central publishes are immutable — no deletes or overwrites; ship a patch.
  Artifacts are fetchable ~15–30 min after release, searchable later.

Release flow:

```bash
# Bump VERSION_NAME in gradle.properties (and the README install snippets).
git add gradle.properties README.md
git commit -m "release v<version>"
git tag v<version>
git push origin main v<version>
```

Always update the SDK version in lockstep with new API surface area added to
the matching `@hot-dev/sdk` JavaScript release when possible.

## Integration Tests

`integration/run.sh` starts a scratch `hot dev` (requires the `hot` CLI on
PATH) serving the Hot fixture project in `integration/fixture`
(project `sdk-fixture`: an echo event handler, an always-failing handler and
function, a handler that fails its first attempt and succeeds on retry
(`sdk:flaky`), a function that streams three chunks, and hot:call targets). It binds 127.0.0.1 on HOT_TEST_PORT (default
4723), mints a full-access API key with `hot key create`,
exports HOT_TEST_BASE_URL / HOT_TEST_API_KEY, and runs `./gradlew test --tests dev.hot.sdk.IntegrationTest` (build.gradle.kts forwards the env vars to the forked test JVM past the Gradle daemon).
The integration tests skip when HOT_TEST_API_KEY is unset, so regular test
runs are unaffected. The fixture pins the API to 127.0.0.1 because binding
"localhost" can yield an IPv6-only listener that JDK-style clients (which
dial the first resolved address only) cannot reach.

## Lockstep Releases

All five SDK repos (hot-js, hot-python, hot-go, hot-rust, hot-java) release in
lockstep. Checklist for a coordinated release:

1. Bump every SDK's version: js `npm version` (syncs src/version.ts via the
   `version` script), python pyproject.toml, rust Cargo.toml, java
   gradle.properties + README snippets, go `Version` constant in client.go.
2. Regenerate each repo's openapi-operations fixture from a current `hot dev`
   and reconcile the coverage tables.
3. Run every repo's `integration/run.sh`.
4. Commit, tag `v<version>`, push in each repo.

The integration fixture (integration/fixture, run.sh) is
duplicated across all five repos; the canonical copy lives in **hot-python** —
change it there first, then sync the others.
