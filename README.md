# hot-java

Official Java SDK for the [Hot Dev](https://hot.dev) API.

- Maven Central: `dev.hot:hot-sdk`
- Package: `dev.hot.sdk`
- Requires Java 17+
- Single runtime dependency (Jackson); HTTP via the JDK's `java.net.http.HttpClient`

## Install

Gradle:

```kotlin
implementation("dev.hot:hot-sdk:1.3.0")
```

Maven:

```xml
<dependency>
  <groupId>dev.hot</groupId>
  <artifactId>hot-sdk</artifactId>
  <version>1.3.0</version>
</dependency>
```

## Quick Start

```java
import dev.hot.sdk.HotClient;
import dev.hot.sdk.StreamEvents;
import java.util.Map;

HotClient client = HotClient.builder(System.getenv("HOT_API_KEY")).build();

// baseUrl defaults to https://api.hot.dev.
// For local development with `hot dev`, use .baseUrl("http://localhost:4681").

try (StreamEvents events = client.streams().subscribeWithEvent(Map.of(
    "event_type", "team-agent:ask",
    "event_data", Map.of(
        "session_id", "web:chat:demo",
        "user_id", "web:user:demo",
        "user_name", "Demo User",
        "question", "what is blocking launch?")))) {
  while (events.hasNext()) {
    Map<String, Object> event = events.next();

    if (StreamEvents.typeOf(event).equals("stream:data")) {
      System.out.println(event.get("data_type") + " " + event.get("payload"));
    }

    if (StreamEvents.typeOf(event).equals("run:stop")) {
      System.out.println(StreamEvents.runOf(event).get("result"));
      break;
    }
  }
}
```

Authenticated clients should run server-side. Browser apps and untrusted
clients should call your own backend route instead of exposing a Hot API key.
Administrative runtime endpoints require an API key with appropriate grants. Sessions and service keys are
permission-scoped and are mainly for event publishing and stream reads.

When code has a run id but does not need the full stream, wait on its durable
terminal snapshot:

```java
Map<String, Object> run = client.runs().waitFor(
    runId,
    new RunWaitOptions().timeout(Duration.ofMinutes(5)));
```

## Async Usage

`AsyncHotClient` wraps a `HotClient` and returns `CompletableFuture`s. Calls
block a thread while they wait, so they run on a dedicated pool of daemon
threads (`hot-sdk-async-N`) by default, not the common pool; pass your own with
`AsyncHotClient.wrap(client, executor)`. Streaming methods deliver events to a
consumer, and cancelling their future (`future.cancel(true)`) closes the
subscription:

```java
import dev.hot.sdk.AsyncHotClient;

AsyncHotClient async = AsyncHotClient.wrap(client);
async.events()
    .publish(Map.of("event_type", "team-agent:ask", "event_data", Map.of("question", "what changed?")))
    .thenAccept(event -> System.out.println(event.get("stream_id")));
```

When a run returns a background task id, wait through the durable task resource:

```java
Map<String, Object> task = client.tasks().waitFor(
    taskId,
    new TaskWaitOptions().timeout(Duration.ofMinutes(5)));
```

The task waiter receives the latest persisted state first and reconnects, so it
cannot miss a task that completed before subscription. A failed, cancelled, or
timed-out task throws `HotTaskException` with the final task record.
The task's parent stream also emits durable `task:update` events, which is
useful when one subscription needs to coordinate several tasks.

## Errors

Non-2xx API responses throw `HotApiException` with structured fields:

```java
import dev.hot.sdk.HotApiException;

try {
  client.projects().get("missing-project");
} catch (HotApiException error) {
  System.out.println(error.statusCode() + " " + error.code() + " "
      + error.requestId() + " " + error.retryAfter());
}
```

A `403 key_scope_mismatch` throws the subclass `HotKeyScopeException`: the call
needs the other kind of API key. `requiredScope()` is `"org"` for `orgs()` and
`apiKeys()` and `"env"` for every runtime resource (see [Org API](#org-api)).

Transport failures throw `HotTransportException`; run failures from
`runs().waitFor`/`waitForRunResult`/`callHot` throw `HotRunException`; task failures throw
`HotTaskException`; wait timeouts throw `HotTimeoutException`. All are unchecked.

### Retries and reconnects

JSON requests are retried automatically (at most twice) when the API responds
429 with a `retry_after`, and GET, PUT and DELETE requests also on 502, 503 and
504; other errors are thrown as-is. Raw requests are never retried.

Durable waiters (`runs().waitFor`, `tasks().waitFor`, `waitForRunResult`,
`callHot`) and `subscribeWithEvent` reconnect across transport errors, 408, 429
and 5xx, with jittered exponential backoff (0.5 s doubling to 10 s) that honours
`Retry-After`. They stop at once on permanent errors: any other 4xx, including
401, 403 and 404. `maxAttempts` (default 5) counts consecutive failed
connections only: the API ends every subscription after 5 minutes with
`stream:complete`, and those rollovers reconnect immediately without using the
budget, so a long run is bounded only by its timeout.

A waiter's timeout is an absolute deadline covering the connection, the wait
for response headers, the stream and any backoff. Connections that receive
nothing for 45 seconds (the API sends a keepalive every 15 s) are treated as
dead and reconnected.

A handler with `retry` options reports a failed attempt as `run:fail` with
status `pending_retry`. Waiters and `subscribeWithEvent` keep going and return
the retry run, which the API links to the first attempt by `origin_run_id`.

### Decimal values

JSON numbers with a fraction decode as `Double` by default, which rounds values
beyond about 16 significant digits (Hot `Dec` results, money in `event_data`).
Build the client with `useBigDecimalForFloats(true)` to decode them as
`BigDecimal` in every response, stream event and run result:

```java
HotClient client = HotClient.builder(token).useBigDecimalForFloats(true).build();
BigDecimal total = (BigDecimal) client.events().callHot("::billing/total", List.of(orderId));
```

## Resources

`HotClient` mirrors the Hot API v1 resources:

- `client.events()` — publish, list, get, inspect event runs, and call Hot functions with `callHot(fn, args, options)`
- `client.streams()` — subscribe to run and task updates, wait for run results, and publish events atomically (reconnects automatically across the 5-minute SSE timeout; use `.reconnect(false)` to opt out)
- `client.runs()` — list, inspect, subscribe to, and wait for durable runs
- `client.tasks()` — get, subscribe to, and wait for durable background tasks
- `client.files()` — upload, download, list, and delete files (including multipart uploads)
- `client.projects()` — create, list, update, activate, deactivate, and delete projects
- `client.builds()` — upload, download, deploy, and look up live/deployed builds
- `client.context()` — manage encrypted project context variables
- `client.domains()` — register, verify, list, and delete custom domains
- `client.sessions()` — create and revoke scoped sessions
- `client.serviceKeys()` — create and revoke scoped service keys
- `client.org()` — view usage and limits
- `client.env()` — read environment info, subscribe to environment events, and purge call traces
- `client.orgs()` — org API: child organizations, environments, and org and environment API keys (org API keys only)
- `client.apiKeys()` — read, narrow, rotate, and revoke API keys by id (org API keys only)

Use `client.request(...)` or `client.requestRaw(...)` as an escape hatch for
API endpoints that do not yet have a resource helper.

`client.env().subscribe()` requires API key credentials and a live API pub/sub
backend; local API servers without pub/sub return a 503.
`subscribeWithEvent` reconnects across the API's 5-minute SSE rollover and
stops after the final run correlated to the event it published. Unrelated
runs on the same stream do not end the iterator.

`onChunk` callbacks receive the run's `stream:data` text in order, even when
the API delivers chunks before `run:start` (hot 3.0.x does for short runs).

## Org API

An **org API key** manages a root organization's children: it creates child
organizations and environments, mints environment API keys for them, deploys
projects into them, and reads account usage. Root administrators create org
keys in the Hot App. An org key calls only `client.orgs()` and
`client.apiKeys()`; every runtime resource (events, streams, runs, projects,
...) needs an **environment API key**, session, or service key. Calling the
other scope's routes throws `HotKeyScopeException`. Use one client per
credential:

```java
HotClient admin = HotClient.builder(System.getenv("HOT_ORG_API_KEY")).build();

Map<String, Object> child = admin.orgs().create(Map.of(
    "name", "Customer", "slug", "customer-123",          // slug: at most 32 characters
    "idempotency_key", "customer-123"));                  // reuse it when retrying
String orgId = (String) ((Map<?, ?>) child.get("org")).get("org_id");
String envId = (String) child.get("env_id");             // the child's default environment

Map<String, Object> key = admin.orgs().createEnvApiKey(orgId, envId, Map.of(
    "description", "Customer backend",
    "permissions", Map.of("event:*", List.of("create", "read"), "stream:*", List.of("read"))));
HotClient customer = HotClient.builder((String) key.get("token")).build();
customer.events().publish(Map.of("event_type", "order:placed", "event_data", Map.of()));
```

| Method | Route |
| --- | --- |
| `orgs().list(query)` / `create(body)` / `get(orgId)` | `/v1/orgs`, `/v1/orgs/{org_id}` |
| `orgs().update(orgId, Map.of("state", "suspended"))` | `active`, `suspended` or `archived`; returns the org |
| `orgs().usage(rootOrgId)` | Account usage; pass the root org id |
| `orgs().listEnvs(orgId)` / `createEnv(orgId, body)` / `getEnv(orgId, envId)` | Environments (creation is idempotent by name) |
| `orgs().listApiKeys(orgId)` / `createApiKey(orgId, body)` | Org API keys in the key's own org |
| `orgs().listEnvApiKeys(orgId, envId)` / `createEnvApiKey(orgId, envId, body)` | Environment API keys |
| `orgs().env(orgId, envId).projects()` / `.builds()` / `.context()` | Projects, builds, deploys and context in a child environment |
| `orgs().purgeCalls(orgId, includeSubOrgs)` | Delete call traces |
| `apiKeys().get(id)` / `update(id, body)` / `rotate(id, body)` / `revoke(id)` | Any key the org key issued |

Minted keys' grants must fit inside the org key's grants. Revoking or narrowing
an org key restricts every key, service key and session issued under it on
their next request. `AsyncHotClient` has the same methods under `orgs()` and
`apiKeys()`. See the server's [org API docs](https://hot.dev/docs/api/management)
for the permission grammar.

### Org API integration tests

See [the validation record](integration/VALIDATION.md) for the tested server revision.

`integration/suborg_fixture.py` creates a disposable database and loopback API
server built from the hot revision in `integration/hot-server.ref`, seeds an org
API key, runs the SDK tests, and removes only its own resources. The canonical
fixture lives in hot-python and is synchronized across all five SDKs. For
PostgreSQL it also needs Docker, `psql` and a hot checkout (default: sibling
`hot`); the API then runs as a non-owner `NOSUPERUSER NOBYPASSRLS` login with
RLS enforced.

```bash
python3 integration/suborg_fixture.py --hot ../hot/target/debug/hot --backend postgres -- \
  ./gradlew test --tests dev.hot.sdk.SuborgIntegrationTest
# For SQLite, use --backend sqlite.
```

Coverage includes child and environment provisioning and idempotency,
identical project names in siblings, foreign-ID and stream denial, key-scope
errors in both directions, org-scoped projects and context, key rotation and
revocation, restricted environment-key grants, service and session issuance
and revocation, revocation and narrowing of an issuing org key, and child
suspension and resumption.

## Customizing the HTTP Client

Pass your own `java.net.http.HttpClient` for proxies or executors:

```java
HotClient client = HotClient.builder(token)
    .httpClient(HttpClient.newBuilder().proxy(mySelector).build())
    .build();
```

The default `HttpClient` has a 10-second connect timeout. Use the builder's
`timeout` (default 60 seconds) for JSON requests. Do not configure a
client-wide request timeout for SSE subscriptions — it would sever long-lived
streams.

## Casing Policy

Core API request and response payloads use the Hot API wire format:
`event_type`, `event_data`, `stream_id`, and so on. SDK-only options use Java
style names such as `baseUrl` and `timeout`.

The SDK never transforms user-owned payloads such as `event_data`.

## Local Development

```bash
./gradlew build     # compiles, tests, jars
./gradlew javadoc
```

## Related

- [Hot Dev documentation](https://hot.dev/docs)
- [SDK docs](https://hot.dev/docs/api/sdks)
- [JavaScript / TypeScript SDK](https://www.npmjs.com/package/@hot-dev/sdk)
- [Python SDK](https://pypi.org/project/hot-dev/)

## License

Apache-2.0
