# hot-java

Official Java SDK for the [Hot Dev](https://hot.dev) API.

- Maven Central: `dev.hot:hot-sdk`
- Package: `dev.hot.sdk`
- Requires Java 17+
- Single runtime dependency (Jackson); HTTP via the JDK's `java.net.http.HttpClient`

## Install

Gradle:

```kotlin
implementation("dev.hot:hot-sdk:1.1.3")
```

Maven:

```xml
<dependency>
  <groupId>dev.hot</groupId>
  <artifactId>hot-sdk</artifactId>
  <version>1.1.3</version>
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
Most management endpoints require an API key. Sessions and service keys are
permission-scoped and are mainly for event publishing and stream reads.

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
`waitForRunResult`/`callHot` throw `HotRunException`; wait timeouts throw
`HotTimeoutException`. All are unchecked.

### Retries

JSON requests are retried automatically (at most twice) when the API responds
429 with a `retry_after`; other errors are thrown as-is. Streaming and raw
requests are never retried.

## Resources

`HotClient` mirrors the Hot API v1 resources:

- `client.events()` — publish, list, get, inspect event runs, and call Hot functions with `callHot(fn, args, options)`
- `client.streams()` — subscribe to run streams, wait for run results, and publish events atomically (reconnects automatically across the 5-minute SSE timeout; use `.reconnect(false)` to opt out)
- `client.runs()` — list, inspect, and view run stats
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
stops after the first terminal `run:*` event it sees, so use
`client.streams().subscribe` directly if your app expects multiple independent
runs on the same stream.

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
- [JavaScript / TypeScript SDK](https://www.npmjs.com/package/@hot-dev/sdk)
- [Python SDK](https://pypi.org/project/hot-dev/)

## Layer 2 Helpers

This SDK intentionally covers the core Hot API (Layer 1). Agent, webhook, and
proxy helpers (Layer 2) currently live in the JavaScript SDK's subpath exports
(`@hot-dev/sdk/agent`, `/webhook`, `/proxy`).

## License

Apache-2.0
