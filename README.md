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

`AsyncHotClient` wraps a `HotClient` and returns `CompletableFuture`s, running
calls on the common pool or an executor you supply; streaming methods deliver
events to a consumer:

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

Transport failures throw `HotTransportException`; run failures from
`runs().waitFor`/`waitForRunResult`/`callHot` throw `HotRunException`; task failures throw
`HotTaskException`; wait timeouts throw `HotTimeoutException`. All are unchecked.

### Retries

JSON requests are retried automatically (at most twice) when the API responds
429 with a `retry_after`; other errors are thrown as-is. Streaming and raw
requests are never retried.

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
- `client.env()` — read environment info and subscribe to environment events

Use `client.request(...)` or `client.requestRaw(...)` as an escape hatch for
API endpoints that do not yet have a resource helper.

`client.env().subscribe()` requires API key credentials and a live API pub/sub
backend; local API servers without pub/sub return a 503.
`subscribeWithEvent` reconnects across the API's 5-minute SSE timeout and
stops after the terminal run correlated to the event it published. Unrelated
runs on the same stream do not end the iterator.

## Customizing the HTTP Client

Pass your own `java.net.http.HttpClient` for proxies or executors:

```java
HotClient client = HotClient.builder(token)
    .httpClient(HttpClient.newBuilder().proxy(mySelector).build())
    .build();
```

Use the builder's `timeout` for JSON requests. Do not configure a client-wide
request timeout for SSE subscriptions — it would sever long-lived streams.

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

## Sub-organization management

Requires a Hot server release containing the sub-org management API. Use the
separate `HotManagementClient` with an `hm_` credential issued by a root admin.
Runtime API keys, service keys, and sessions cannot call management endpoints;
management credentials cannot call runtime endpoints. Create a runtime client
per environment credential. A root environment key cannot access its children,
and a child key cannot access siblings. There is no tenant or org selector on
runtime methods. Each child owns its environments and deployments.

```java
HotManagementClient management = HotManagementClient.builder(managementToken).build();
Map<String, Object> child = management.createChild(Map.of(
    "name", "Customer", "slug", "customer-unique-slug",
    "idempotency_key", "customer-provisioning-123"));
Map<?, ?> org = (Map<?, ?>) child.get("org");
Map<String, Object> key = management.createApiKey(
    (String) org.get("org_id"), (String) child.get("env_id"), Map.of(
        "description", "Customer backend", "permissions", Map.of("*:*", List.of("*"))));
HotClient customer = HotClient.builder((String) key.get("token")).build();
System.out.println(customer.env().get());
```

`AsyncHotManagementClient.wrap(management)` offers the same methods as futures,
with an optional executor, matching `AsyncHotClient`.

| Management method | Purpose |
| --- | --- |
| `listChildren(query)` | List authorized children (returns `children` and `total`) |
| `createChild(body)` | Provision a child and default environment |
| `setChildState(childId, state)` | Set `active`, `suspended`, or `archived` (no response body) |
| `createEnvironment(childId, body)` | Provision a child environment by name |
| `createApiKey(childId, envId, body)` | Issue a child API key within the delegation ceiling |
| `usage()` | Read account-wide usage |

Child creation requires a stable `idempotency_key`; reuse it for retries of the
same request. A different payload with that key is rejected. Environment
creation is idempotent within a child by exact name. Listing accepts `limit`
(maximum 500) and `offset`. Account usage requires `usage:read` and explicit
`all_children` authority. Management credentials cannot issue other management
credentials or grant human access. Service keys and sessions are issued with the
child environment's runtime API key and inherit its management issuer chain.
Issuer expiry, revocation, or permission narrowing restrict existing descendants;
expansion never expands their stored grants.

Administrative runtime operations require an API key **and** the appropriate
resource/action grants. For example, project listing requires `project:*` with
`read`, and environment info and usage require `env:{env_id}` with `read`.
Event-only keys receive `403` for these operations; API-key status alone does
not grant administration. Full-access `{"*:*": ["*"]}` keys retain access.
See the server's [permission contract](https://hot.dev/docs/api/management).

`org.usage` reports only the credential's **environment consumption**, with
`env_id`, `usage_scope: "environment"`, and `limits_scope: "billing_account"`.
Its limits and plan come from the billing root; percentages compare this one
environment against shared limits and do not describe remaining account quota.
`usage.team_members` is zero because seats belong to the account. Use management
`usage` for the account aggregate. Existing runtime endpoints and request bodies
keep their shapes. JSON requests retain bounded 429/Retry-After handling;
streaming reconnects and waiters stop on permanent `401`/`403` authorization errors.

### Sub-org integration tests

See [the validation record](integration/VALIDATION.md) for the tested server revision and results.

The fixture creates an owned disposable database and loopback API server, runs
the SDK tests, and removes only its own resources. The canonical fixture lives
in hot-python and is synchronized across all five SDKs. No deployed credentials
are used. Requires Python 3 and a Hot CLI built from the matching server branch.
For PostgreSQL, also install Docker and `psql` and provide the public hot checkout
(default: sibling `hot`). Migrations use a non-superuser schema owner; the API
uses a distinct non-owner `NOSUPERUSER NOBYPASSRLS` login with RLS enforcement.
The fixture's administrator seeds credentials only; SDK calls always use HTTP.

```bash
python3 integration/suborg_fixture.py --hot ../hot/target/debug/hot --backend postgres -- \
  ./gradlew test --tests dev.hot.sdk.SuborgIntegrationTest
# For SQLite, use --backend sqlite.
```

Coverage includes provisioning and environment idempotency, identical project
names in siblings, foreign-ID and stream denial, parent runtime isolation,
management/runtime token separation, usage scope, restricted API-key grants,
service/session issuance and revocation, pre-existing descendant issuer
revocation/narrowing, and child suspension/resumption.
