package dev.hot.sdk;

import dev.hot.sdk.internal.Transport;
import java.io.InputStream;
import java.net.http.HttpClient;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * Synchronous Hot API v1 client.
 *
 * <p>Request and response payloads use the Hot API wire format ({@code event_type},
 * {@code stream_id}, {@code event_data}, ...) as {@code Map<String, Object>} values; the SDK
 * never transforms user-owned payloads such as {@code event_data}. SDK-only options use
 * Java-idiomatic names ({@code baseUrl}, {@code timeout}).
 *
 * <pre>{@code
 * HotClient client = HotClient.builder(System.getenv("HOT_API_KEY")).build();
 * try (StreamEvents events = client.streams().subscribeWithEvent(Map.of(
 *     "event_type", "team-agent:ask",
 *     "event_data", Map.of("question", "what is blocking launch?")))) {
 *   while (events.hasNext()) {
 *     Map<String, Object> event = events.next();
 *     if (StreamEvents.typeOf(event).equals("run:stop")) {
 *       System.out.println(StreamEvents.runOf(event).get("result"));
 *       break;
 *     }
 *   }
 * }
 * }</pre>
 *
 * <p>Authenticated clients should run server-side; browser or untrusted clients should call your
 * own backend instead of embedding a Hot API key.
 *
 * <p>Hot has two key scopes. Environment API keys (and the sessions and service keys they issue)
 * call runtime resources such as {@link #events()}, {@link #streams()} and {@link #projects()}.
 * Org API keys call only {@link #orgs()} and {@link #apiKeys()}. Calling the other kind's routes
 * throws {@link HotKeyScopeException}; use one client per credential.
 */
public final class HotClient {

  private final Transport transport;
  private final EventsResource events;
  private final RunsResource runs;
  private final EnvResource env;
  private final OrgResource org;
  private final StreamsResource streams;
  private final TasksResource tasks;
  private final FilesResource files;
  private final BlobsResource blobs;
  private final ProjectsResource projects;
  private final BuildsResource builds;
  private final ContextResource context;
  private final DomainsResource domains;
  private final SessionsResource sessions;
  private final ServiceKeysResource serviceKeys;
  private final OrgsResource orgs;
  private final ApiKeysResource apiKeys;

  HotClient(Transport transport) {
    this.transport = transport;
    this.events = new EventsResource(transport);
    this.runs = new RunsResource(transport);
    this.env = new EnvResource(transport);
    this.org = new OrgResource(transport);
    this.streams = new StreamsResource(transport);
    this.tasks = new TasksResource(transport);
    this.files = new FilesResource(transport);
    this.blobs = new BlobsResource(transport);
    this.projects = new ProjectsResource(transport);
    this.builds = new BuildsResource(transport);
    this.context = new ContextResource(transport);
    this.domains = new DomainsResource(transport);
    this.sessions = new SessionsResource(transport);
    this.serviceKeys = new ServiceKeysResource(transport);
    this.orgs = new OrgsResource(transport);
    this.apiKeys = new ApiKeysResource(transport);
  }

  /** Returns a builder using the given Hot API key, session token, or service key. */
  public static Builder builder(String token) {
    return new Builder(token);
  }

  /** The configured base URL without the /v1 suffix. */
  public String baseUrl() {
    return transport.baseUrl();
  }

  public EventsResource events() {
    return events;
  }

  public RunsResource runs() {
    return runs;
  }

  public EnvResource env() {
    return env;
  }

  public OrgResource org() {
    return org;
  }

  public StreamsResource streams() {
    return streams;
  }

  public TasksResource tasks() {
    return tasks;
  }

  public FilesResource files() {
    return files;
  }

  /** Large payloads spilled to blob storage (environment API keys only). */
  public BlobsResource blobs() {
    return blobs;
  }

  public ProjectsResource projects() {
    return projects;
  }

  public BuildsResource builds() {
    return builds;
  }

  public ContextResource context() {
    return context;
  }

  public DomainsResource domains() {
    return domains;
  }

  public SessionsResource sessions() {
    return sessions;
  }

  public ServiceKeysResource serviceKeys() {
    return serviceKeys;
  }

  /** Org API (org API keys only): child organizations, environments and API keys. */
  public OrgsResource orgs() {
    return orgs;
  }

  /** Org and environment API keys by id (org API keys only). */
  public ApiKeysResource apiKeys() {
    return apiKeys;
  }

  /**
   * Calls a JSON API endpoint that does not yet have a resource helper. {@code path} is relative
   * to /v1 and the returned map is the response envelope.
   */
  public Map<String, Object> request(
      String method, String path, Object body, Map<String, String> query) {
    return transport.requestJson(method, path, body, query);
  }

  /**
   * Calls an API endpoint and returns the raw HTTP response. The caller must close the body
   * stream. Non-2xx responses become {@link HotApiException}.
   */
  public HttpResponse<InputStream> requestRaw(
      String method,
      String path,
      Map<String, String> query,
      Map<String, String> headers,
      BodyPublisher body) {
    return transport.requestRaw(method, path, query, headers, body);
  }

  Transport transport() {
    return transport;
  }

  /** Builder for {@link HotClient}. */
  public static final class Builder {

    private final String token;
    private String baseUrl = Transport.DEFAULT_BASE_URL;
    private Duration timeout;
    private HttpClient httpClient;
    private boolean bigDecimals;

    Builder(String token) {
      this.token = token;
    }

    /**
     * Overrides the base URL (default {@code https://api.hot.dev}). For local development with
     * {@code hot dev}, use {@code http://localhost:4681}.
     */
    public Builder baseUrl(String baseUrl) {
      this.baseUrl = baseUrl;
      return this;
    }

    /**
     * Bounds each JSON request (default 60 seconds). Not applied to streaming or raw requests;
     * waiters take their own timeout.
     */
    public Builder timeout(Duration timeout) {
      this.timeout = timeout;
      return this;
    }

    /**
     * Overrides the underlying {@link HttpClient}, e.g. to configure proxies or executors. The
     * default client has a 10-second connect timeout.
     */
    public Builder httpClient(HttpClient httpClient) {
      this.httpClient = httpClient;
      return this;
    }

    /**
     * Decodes JSON numbers with a fraction or exponent as {@link java.math.BigDecimal} instead of
     * {@code Double} (default false), in responses, stream events and run results. Hot {@code
     * Dec} values and user payloads such as {@code event_data} then keep every digit. Integers
     * decode as {@code Integer}, {@code Long} or {@code BigInteger} either way.
     */
    public Builder useBigDecimalForFloats(boolean enabled) {
      this.bigDecimals = enabled;
      return this;
    }

    public HotClient build() {
      return new HotClient(new Transport(token, baseUrl, timeout, httpClient, bigDecimals));
    }
  }
}
