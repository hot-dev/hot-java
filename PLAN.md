# hot-java — Implementation Plan

Official Java SDK for the Hot Dev API. Mirrors `hot-python` (Layer 1 only: client,
resources, SSE streaming, errors) and follows the shared SDK conventions.

## Package identity

- Maven coordinates: **`dev.hot:hot-sdk`** (the `dev.hot` groupId has no existing
  artifacts on Central — confirmed 2026-07-15 — and maps to the hot.dev domain,
  which is what Central's namespace verification requires).
- Java package: `dev.hot.sdk`.
- License: Apache-2.0.
- Java baseline: **17** (current sane LTS floor; toolchain can build on 21).
- Versioning: lockstep with the other SDKs; tags `v<version>`.

## Tech choices

- **Build**: Gradle (Kotlin DSL) with the wrapper checked in.
- **HTTP**: `java.net.http.HttpClient` (JDK built-in) — keeps the dependency
  footprint to JSON only. Users can inject their own configured `HttpClient`
  (mirrors the `httpx.Client` injection in Python).
- **JSON**: Jackson (`jackson-databind`) — the only runtime dependency.
- **Typing policy**: match Python's v1 approach — payloads are
  `Map<String, Object>` / `JsonNode`; typed POJOs generated from the OpenAPI
  spec are a later release. The SDK never transforms user-owned payloads
  (`event_data`).
- **Sync + async**: `HotClient` (blocking) and `AsyncHotClient`
  (`CompletableFuture`-returning) sharing one transport, same split as Python.
  SSE subscriptions: blocking client returns `Stream<StreamEvent>` /
  `Iterator`; async client takes a consumer or returns a `Flow.Publisher`
  (decide during phase 4 — `Iterator` on a dedicated thread is simplest and fine
  for v1).
- **Errors**: unchecked `HotApiException` with `statusCode`, `code`,
  `requestId`, `retryAfter` accessors; `HotTransportException` for I/O.
- **Config**: `HotClient.builder().token(...).baseUrl(...).timeout(...).httpClient(...).build()`;
  `baseUrl` defaults to `https://api.hot.dev`. Both clients are `AutoCloseable`.

## Architecture

```
dev/hot/sdk/
  HotClient.java, AsyncHotClient.java   — builders, resource accessors,
                                          request()/requestRaw() escape hatches
  internal/Transport.java               — auth header, JSON round-trip, error mapping
  HotApiException.java
  internal/SseParser.java               — SSE parser over HttpResponse.BodyHandlers.ofLines()
  StreamEvent.java                      — event envelope (type, dataType, payload, run)
  resources/                            — Builds, Context, Domains, Env, Events, Files,
                                          Org, Projects, Runs, ServiceKeys, Sessions,
                                          Streams (12 namespaces, ~59 operations)
```

Behavioral contract to preserve:
- `streams.subscribe*` auto-reconnects across the API's 5-minute SSE timeout
  (opt-out flag); `subscribeWithEvent` ends after the first terminal `run:*` event.
- Casing policy: wire format in payload maps (`event_type`, `stream_id`);
  Java-idiomatic names for SDK-only options (`baseUrl`, `timeout`).
- `files` multipart upload/download; `builds` upload/download via `InputStream`/`Path`.

## Implementation phases

1. Scaffold: Gradle project, wrapper, CI, LICENSE, README, AGENTS.md, Spotless
   (google-java-format).
2. Transport + exceptions + escape-hatch `request`/`requestRaw`.
3. Resource namespaces (port from `hot-python/src/hot/resources.py`).
4. SSE parser + reconnect + `subscribeWithEvent` + `waitForRunResult`; async client.
5. Docs: Javadoc on the public API, README quick start mirroring JS/Python.
6. OpenAPI coverage test + fixture.

## Testing

- **Unit/integration**: JUnit 5 + **WireMock** for mocked HTTP, including chunked
  SSE bodies and dropped connections for reconnect tests (mirror
  `test_reconnect.py` / `reconnect.test.ts`).
- **OpenAPI coverage**: check in `src/test/resources/openapi-operations.json`
  (same format as the other SDKs) + a small updater script/task fetching
  `http://localhost:4681/openapi.json`; test asserts every operation has a
  resource method.
- **Live smoke test** (optional): `@EnabledIfEnvironmentVariable(named = "HOT_API_KEY")`.
- CI: `./gradlew spotlessCheck build` (compiles, tests, javadoc) on JDK 17 and 21,
  pushes/PRs to `main`.

## Publishing (Maven Central — the most setup-heavy of the three)

Central publishing now goes through the **Central Portal**
(central.sonatype.com); the legacy OSSRH/oss.sonatype.org path was sunset in
June 2025 — ignore any docs referencing Nexus staging repos.

One-time setup (Curt):
1. Create the public GitHub repo `hot-dev/hot-java`.
2. Create a Central Portal account at central.sonatype.com.
3. Register the **`dev.hot` namespace** and verify it with a DNS TXT record on
   `hot.dev` (the portal gives you the token to publish; verification is quick
   since you control the domain).
4. Generate a **GPG keypair** for artifact signing and push the public key to
   `keyserver.ubuntu.com`. Central still requires signed artifacts.
5. Generate a Portal **user token** (username + password pair).
6. Add repo secrets: `MAVEN_CENTRAL_USERNAME`, `MAVEN_CENTRAL_PASSWORD`,
   `SIGNING_KEY` (ASCII-armored private key), `SIGNING_PASSWORD`.

Build requirements Central validates on every publish:
- `-sources.jar` and `-javadoc.jar` alongside the main jar
- Complete POM: name, description, url, license, developers, scm
- GPG signatures on everything

Recommended plugin: **`com.vanniktech.maven.publish`** — one plugin that handles
sources/javadoc jars, signing, POM wiring, and Central Portal upload
(`publishToMavenCentral` with automatic release). This is much less fiddly than
hand-rolling `maven-publish` + `signing` + a portal uploader.

Release workflow (`.github/workflows/release.yml`, on `v*` tags):
- Validate tag matches the version in `gradle.properties` (same guard as hot-js).
- `./gradlew spotlessCheck build`, then `./gradlew publishToMavenCentral`.
- Central validates asynchronously; the artifact is fetchable in ~15–30 min,
  visible on search.maven.org within a few hours. Publishes are **immutable** —
  no deletes or overwrites; ship a patch to fix mistakes.

Release flow: bump version in `gradle.properties` → commit `release v<version>`
→ tag → push.

## Open questions

- Kotlin-friendliness: plain Java is fine for v1; revisit extension helpers if
  Kotlin users ask.
- Async SSE shape (`Iterator`-on-thread vs `Flow.Publisher`) — decide in phase 4.
