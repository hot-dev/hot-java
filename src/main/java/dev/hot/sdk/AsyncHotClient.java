package dev.hot.sdk;

import java.io.InputStream;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Asynchronous Hot API v1 client returning {@link CompletableFuture}s. A thin wrapper over
 * {@link HotClient} that runs each call on the configured executor; streaming methods drive the
 * event iterator on that executor and deliver events to a consumer.
 *
 * <p>Calls block a thread for the whole request or wait, so the default executor is a bounded
 * pool of daemon threads ({@code hot-sdk-async-N}) that scales down to zero when idle, never
 * {@link java.util.concurrent.ForkJoinPool#commonPool()}. Pass your own with {@link
 * #wrap(HotClient, Executor)}.
 *
 * <p>Cancelling a streaming method's future ({@code future.cancel(true)}) interrupts any in-flight
 * connect or reconnect sleep, closes the underlying subscription, and stops delivering events.
 */
public final class AsyncHotClient {

  private static final AtomicInteger THREADS = new AtomicInteger();

  static final int DEFAULT_MAX_THREADS =
      Math.min(64, Math.max(16, Runtime.getRuntime().availableProcessors() * 4));

  // Lazily created: clients wrapping their own executor never start it.
  private static final class DefaultExecutor {
    static final ThreadPoolExecutor INSTANCE = create();

    private static ThreadPoolExecutor create() {
      ThreadPoolExecutor executor =
          new ThreadPoolExecutor(
              DEFAULT_MAX_THREADS,
              DEFAULT_MAX_THREADS,
              60L,
              TimeUnit.SECONDS,
              new LinkedBlockingQueue<>(),
              runnable -> {
                Thread thread =
                    new Thread(runnable, "hot-sdk-async-" + THREADS.incrementAndGet());
                thread.setDaemon(true);
                return thread;
              });
      executor.allowCoreThreadTimeOut(true);
      return executor;
    }
  }

  private final HotClient client;
  private final Executor executor;
  private final Events events;
  private final Runs runs;
  private final Env env;
  private final Org org;
  private final Streams streams;
  private final Tasks tasks;
  private final Files files;
  private final Blobs blobs;
  private final Projects projects;
  private final Builds builds;
  private final Context context;
  private final Domains domains;
  private final Sessions sessions;
  private final ServiceKeys serviceKeys;
  private final Orgs orgs;
  private final ApiKeys apiKeys;

  private AsyncHotClient(HotClient client, Executor executor) {
    if (client == null) {
      throw new IllegalArgumentException("client is required");
    }
    if (executor == null) {
      throw new IllegalArgumentException("executor is required");
    }
    this.client = client;
    this.executor = executor;
    this.events = new Events();
    this.runs = new Runs();
    this.env = new Env();
    this.org = new Org();
    this.streams = new Streams();
    this.tasks = new Tasks();
    this.files = new Files();
    this.blobs = new Blobs();
    this.projects = new Projects(client.projects());
    this.builds = new Builds(client.builds());
    this.context = new Context(client.context());
    this.domains = new Domains();
    this.sessions = new Sessions();
    this.serviceKeys = new ServiceKeys();
    this.orgs = new Orgs();
    this.apiKeys = new ApiKeys();
  }

  /** Wraps a synchronous client with the SDK's bounded daemon thread pool. */
  public static AsyncHotClient wrap(HotClient client) {
    return wrap(client, DefaultExecutor.INSTANCE);
  }

  /** Wraps a synchronous client with an explicit executor. */
  public static AsyncHotClient wrap(HotClient client, Executor executor) {
    return new AsyncHotClient(client, executor);
  }

  /** Package-private accessor for tests verifying default executor bounds. */
  static ThreadPoolExecutor defaultExecutor() {
    return DefaultExecutor.INSTANCE;
  }

  /** The underlying synchronous client. */
  public HotClient sync() {
    return client;
  }

  /** Calls the public {@code GET /status} health endpoint. */
  public CompletableFuture<Map<String, Object>> status() {
    return supply(client::status);
  }

  private <T> CompletableFuture<T> supply(Supplier<T> supplier) {
    return CompletableFuture.supplyAsync(supplier, executor);
  }

  private CompletableFuture<Void> run(Runnable runnable) {
    return CompletableFuture.runAsync(runnable, executor);
  }

  /**
   * Opens a subscription and delivers its events on the executor. Cancelling the returned future
   * closes the subscription and interrupts any blocked connect or backoff sleep.
   */
  private CompletableFuture<Void> drain(
      Supplier<StreamEvents> open, Consumer<Map<String, Object>> onEvent) {
    CompletableFuture<Void> result = new CompletableFuture<>();
    AtomicReference<StreamEvents> current = new AtomicReference<>();
    AtomicReference<Thread> worker = new AtomicReference<>();
    Object lock = new Object();
    result.whenComplete(
        (ignored, error) -> {
          if (!result.isCancelled()) {
            return;
          }
          StreamEvents source;
          synchronized (lock) {
            source = current.get();
            Thread thread = worker.get();
            if (thread != null) {
              thread.interrupt();
            }
          }
          if (source != null) {
            source.close();
          }
        });
    try {
      executor.execute(
          () -> {
            synchronized (lock) {
              if (result.isDone()) {
                return;
              }
              worker.set(Thread.currentThread());
            }
            try (StreamEvents source = open.get()) {
              boolean cancelled;
              synchronized (lock) {
                current.set(source);
                cancelled = result.isCancelled();
              }
              if (cancelled) {
                return;
              }
              while (!result.isDone() && source.hasNext()) {
                onEvent.accept(source.next());
              }
              result.complete(null);
            } catch (Throwable error) {
              result.completeExceptionally(error);
            } finally {
              synchronized (lock) {
                worker.set(null);
              }
              // Clear any interrupt flag raised by cancellation before returning thread to pool.
              Thread.interrupted();
            }
          });
    } catch (RuntimeException rejected) {
      result.completeExceptionally(rejected);
    }
    return result;
  }

  public Events events() {
    return events;
  }

  public Runs runs() {
    return runs;
  }

  public Env env() {
    return env;
  }

  public Org org() {
    return org;
  }

  public Streams streams() {
    return streams;
  }

  public Tasks tasks() {
    return tasks;
  }

  public Files files() {
    return files;
  }

  /** Large payloads spilled to blob storage (environment API keys only). */
  public Blobs blobs() {
    return blobs;
  }

  public Projects projects() {
    return projects;
  }

  public Builds builds() {
    return builds;
  }

  public Context context() {
    return context;
  }

  public Domains domains() {
    return domains;
  }

  public Sessions sessions() {
    return sessions;
  }

  public ServiceKeys serviceKeys() {
    return serviceKeys;
  }

  public Orgs orgs() {
    return orgs;
  }

  public ApiKeys apiKeys() {
    return apiKeys;
  }

  /** Async escape hatch mirroring {@link HotClient#request}. */
  public CompletableFuture<Map<String, Object>> request(
      String method, String path, Object body, Map<String, String> query) {
    return supply(() -> client.request(method, path, body, query));
  }

  /** Async escape hatch mirroring {@link HotClient#requestRaw}. */
  public CompletableFuture<HttpResponse<InputStream>> requestRaw(
      String method,
      String path,
      Map<String, String> query,
      Map<String, String> headers,
      BodyPublisher body) {
    return supply(() -> client.requestRaw(method, path, query, headers, body));
  }

  /** Async view of {@link EventsResource}. */
  public final class Events {
    public CompletableFuture<Map<String, Object>> publish(Map<String, Object> body) {
      return supply(() -> client.events().publish(body));
    }

    public CompletableFuture<Object> callHot(String fn, List<Object> args) {
      return supply(() -> client.events().callHot(fn, args));
    }

    public CompletableFuture<Object> callHot(String fn, List<Object> args, CallOptions options) {
      return supply(() -> client.events().callHot(fn, args, options));
    }

    public CompletableFuture<Map<String, Object>> list(Map<String, String> query) {
      return supply(() -> client.events().list(query));
    }

    public CompletableFuture<Map<String, Object>> get(String eventId) {
      return supply(() -> client.events().get(eventId));
    }

    public CompletableFuture<Map<String, Object>> getRuns(String eventId) {
      return supply(() -> client.events().getRuns(eventId));
    }
  }

  /** Async view of {@link RunsResource}. */
  public final class Runs {
    public CompletableFuture<Map<String, Object>> purgeCalls(String runId) {
      return supply(() -> client.runs().purgeCalls(runId));
    }

    public CompletableFuture<Map<String, Object>> list(Map<String, String> query) {
      return supply(() -> client.runs().list(query));
    }

    public CompletableFuture<Map<String, Object>> stats() {
      return supply(() -> client.runs().stats());
    }

    public CompletableFuture<Map<String, Object>> get(String runId) {
      return supply(() -> client.runs().get(runId));
    }

    public CompletableFuture<Map<String, Object>> waitFor(String runId) {
      return supply(() -> client.runs().waitFor(runId));
    }

    public CompletableFuture<Map<String, Object>> waitFor(
        String runId, RunWaitOptions options) {
      return supply(() -> client.runs().waitFor(runId, options));
    }

    /** Streams durable run snapshots to {@code onEvent} until the run is terminal. */
    public CompletableFuture<Void> subscribe(
        String runId, Consumer<Map<String, Object>> onEvent) {
      return drain(() -> client.runs().subscribe(runId), onEvent);
    }
  }

  /** Async view of {@link EnvResource}. */
  public final class Env {
    public CompletableFuture<Map<String, Object>> get() {
      return supply(() -> client.env().get());
    }

    public CompletableFuture<Map<String, Object>> purgeCalls() {
      return supply(() -> client.env().purgeCalls());
    }

    /** Streams environment events to {@code onEvent} until the subscription ends. */
    public CompletableFuture<Void> subscribe(Consumer<Map<String, Object>> onEvent) {
      return drain(() -> client.env().subscribe(), onEvent);
    }
  }

  /** Async view of {@link OrgResource}. */
  public final class Org {
    public CompletableFuture<Map<String, Object>> usage() {
      return supply(() -> client.org().usage());
    }
  }

  /** Async view of {@link StreamsResource}. */
  public final class Streams {
    /** Streams events to {@code onEvent} until the subscription ends. */
    public CompletableFuture<Void> subscribe(
        String streamId, String project, Consumer<Map<String, Object>> onEvent) {
      return drain(() -> client.streams().subscribe(streamId, project), onEvent);
    }

    /** Streams events via POST to {@code onEvent} until the subscription ends. */
    public CompletableFuture<Void> subscribePost(
        String streamId, String project, Consumer<Map<String, Object>> onEvent) {
      return drain(() -> client.streams().subscribePost(streamId, project), onEvent);
    }

    public CompletableFuture<Map<String, Object>> purgeCalls(String streamId) {
      return supply(() -> client.streams().purgeCalls(streamId));
    }

    public CompletableFuture<Map<String, Object>> waitForRunResult(
        String streamId, String eventId) {
      return supply(() -> client.streams().waitForRunResult(streamId, eventId));
    }

    public CompletableFuture<Map<String, Object>> waitForRunResult(
        String streamId, String eventId, WaitOptions options) {
      return supply(() -> client.streams().waitForRunResult(streamId, eventId, options));
    }

    /**
     * Atomically subscribes then publishes, streaming events to {@code onEvent} until the run
     * completes (reconnecting per {@code options}).
     */
    public CompletableFuture<Void> subscribeWithEvent(
        Map<String, Object> body,
        SubscribeWithEventOptions options,
        Consumer<Map<String, Object>> onEvent) {
      return drain(() -> client.streams().subscribeWithEvent(body, options), onEvent);
    }
  }

  /** Async view of {@link TasksResource}. */
  public final class Tasks {
    public CompletableFuture<Map<String, Object>> get(String taskId) {
      return supply(() -> client.tasks().get(taskId));
    }

    public CompletableFuture<Map<String, Object>> waitFor(String taskId) {
      return supply(() -> client.tasks().waitFor(taskId));
    }

    public CompletableFuture<Map<String, Object>> waitFor(
        String taskId, TaskWaitOptions options) {
      return supply(() -> client.tasks().waitFor(taskId, options));
    }

    public CompletableFuture<Void> subscribe(
        String taskId, Consumer<Map<String, Object>> onEvent) {
      return drain(() -> client.tasks().subscribe(taskId), onEvent);
    }
  }

  /** Async view of {@link FilesResource}. */
  public final class Files {
    public CompletableFuture<Map<String, Object>> list(Map<String, String> query) {
      return supply(() -> client.files().list(query));
    }

    public CompletableFuture<Map<String, Object>> get(String fileId) {
      return supply(() -> client.files().get(fileId));
    }

    public CompletableFuture<Void> delete(String fileId) {
      return run(() -> client.files().delete(fileId));
    }

    /** Returns the raw HTTP response streaming the file content. Caller must close the stream. */
    public CompletableFuture<HttpResponse<InputStream>> download(String fileId) {
      return supply(() -> client.files().download(fileId));
    }

    public CompletableFuture<Map<String, Object>> upload(
        String path, byte[] content, String contentType) {
      return supply(() -> client.files().upload(path, content, contentType));
    }

    public CompletableFuture<Map<String, Object>> initiateUpload(Map<String, Object> body) {
      return supply(() -> client.files().initiateUpload(body));
    }

    public CompletableFuture<Map<String, Object>> uploadPart(
        String uploadId, int partNumber, byte[] content) {
      return supply(() -> client.files().uploadPart(uploadId, partNumber, content));
    }

    public CompletableFuture<Map<String, Object>> completeUpload(String uploadId) {
      return supply(() -> client.files().completeUpload(uploadId));
    }

    public CompletableFuture<Void> abortUpload(String uploadId) {
      return run(() -> client.files().abortUpload(uploadId));
    }
  }

  /** Async view of {@link BlobsResource}. */
  public final class Blobs {
    /** Returns the raw HTTP response streaming the blob bytes. Caller must close the stream. */
    public CompletableFuture<HttpResponse<InputStream>> download(String blobRefId) {
      return supply(() -> client.blobs().download(blobRefId));
    }
  }

  /** Async view of {@link ProjectsResource}. */
  public final class Projects {
    private final ProjectsResource resource;

    private Projects(ProjectsResource resource) {
      this.resource = resource;
    }

    public CompletableFuture<Map<String, Object>> list(Map<String, String> query) {
      return supply(() -> resource.list(query));
    }

    public CompletableFuture<Map<String, Object>> create(Map<String, Object> body) {
      return supply(() -> resource.create(body));
    }

    public CompletableFuture<Map<String, Object>> get(String project) {
      return supply(() -> resource.get(project));
    }

    public CompletableFuture<Map<String, Object>> update(String project, Map<String, Object> body) {
      return supply(() -> resource.update(project, body));
    }

    public CompletableFuture<Void> delete(String project) {
      return run(() -> resource.delete(project));
    }

    public CompletableFuture<Map<String, Object>> activate(String project) {
      return supply(() -> resource.activate(project));
    }

    public CompletableFuture<Map<String, Object>> deactivate(String project) {
      return supply(() -> resource.deactivate(project));
    }

    public CompletableFuture<Map<String, Object>> eventHandlers(String project) {
      return supply(() -> resource.eventHandlers(project));
    }

    public CompletableFuture<Map<String, Object>> schedules(String project) {
      return supply(() -> resource.schedules(project));
    }
  }

  /** Async view of {@link BuildsResource}. */
  public final class Builds {
    private final BuildsResource resource;

    private Builds(BuildsResource resource) {
      this.resource = resource;
    }

    public CompletableFuture<Map<String, Object>> list(Map<String, String> query) {
      return supply(() -> resource.list(query));
    }

    public CompletableFuture<Map<String, Object>> listForProject(
        String project, Map<String, String> query) {
      return supply(() -> resource.listForProject(project, query));
    }

    public CompletableFuture<Map<String, Object>> get(String project, String buildId) {
      return supply(() -> resource.get(project, buildId));
    }

    public CompletableFuture<Map<String, Object>> deployed(String project) {
      return supply(() -> resource.deployed(project));
    }

    public CompletableFuture<Map<String, Object>> live(String project) {
      return supply(() -> resource.live(project));
    }

    public CompletableFuture<Map<String, Object>> upload(String project, BuildUpload upload) {
      return supply(() -> resource.upload(project, upload));
    }

    /** Returns the raw HTTP response streaming the build archive. Caller must close the stream. */
    public CompletableFuture<HttpResponse<InputStream>> download(String project, String buildId) {
      return supply(() -> resource.download(project, buildId));
    }

    public CompletableFuture<Map<String, Object>> deploy(String project, String buildId) {
      return supply(() -> resource.deploy(project, buildId));
    }
  }

  /** Async view of {@link ContextResource}. */
  public final class Context {
    private final ContextResource resource;

    private Context(ContextResource resource) {
      this.resource = resource;
    }

    public CompletableFuture<Map<String, Object>> list(String project) {
      return supply(() -> resource.list(project));
    }

    public CompletableFuture<Map<String, Object>> create(String project, Map<String, Object> body) {
      return supply(() -> resource.create(project, body));
    }

    public CompletableFuture<Map<String, Object>> update(
        String project, String key, Map<String, Object> body) {
      return supply(() -> resource.update(project, key, body));
    }

    public CompletableFuture<Void> delete(String project, String key) {
      return run(() -> resource.delete(project, key));
    }
  }

  /** Async view of {@link DomainsResource}. */
  public final class Domains {
    public CompletableFuture<Map<String, Object>> create(Map<String, Object> body) {
      return supply(() -> client.domains().create(body));
    }

    public CompletableFuture<Map<String, Object>> list() {
      return supply(() -> client.domains().list());
    }

    public CompletableFuture<Map<String, Object>> get(String domainId) {
      return supply(() -> client.domains().get(domainId));
    }

    public CompletableFuture<Void> delete(String domainId) {
      return run(() -> client.domains().delete(domainId));
    }

    public CompletableFuture<Map<String, Object>> verify(String domainId) {
      return supply(() -> client.domains().verify(domainId));
    }
  }

  /** Async view of {@link SessionsResource}. */
  public final class Sessions {
    public CompletableFuture<Map<String, Object>> create(Map<String, Object> body) {
      return supply(() -> client.sessions().create(body));
    }

    public CompletableFuture<Map<String, Object>> list(Map<String, String> query) {
      return supply(() -> client.sessions().list(query));
    }

    public CompletableFuture<Void> revoke(String sessionId) {
      return run(() -> client.sessions().revoke(sessionId));
    }

    public CompletableFuture<Map<String, Object>> revokeAll() {
      return supply(() -> client.sessions().revokeAll());
    }
  }

  /** Async view of {@link ServiceKeysResource}. */
  public final class ServiceKeys {
    public CompletableFuture<Map<String, Object>> create(Map<String, Object> body) {
      return supply(() -> client.serviceKeys().create(body));
    }

    public CompletableFuture<Map<String, Object>> list(Map<String, String> query) {
      return supply(() -> client.serviceKeys().list(query));
    }

    public CompletableFuture<Map<String, Object>> get(String serviceKeyId) {
      return supply(() -> client.serviceKeys().get(serviceKeyId));
    }

    public CompletableFuture<Map<String, Object>> update(
        String serviceKeyId, Map<String, Object> body) {
      return supply(() -> client.serviceKeys().update(serviceKeyId, body));
    }

    public CompletableFuture<Void> revoke(String serviceKeyId) {
      return run(() -> client.serviceKeys().revoke(serviceKeyId));
    }

    public CompletableFuture<Map<String, Object>> revokeAll() {
      return supply(() -> client.serviceKeys().revokeAll());
    }
  }

  /** Async view of {@link OrgsResource}. */
  public final class Orgs {
    public CompletableFuture<Map<String, Object>> list(Map<String, String> query) {
      return supply(() -> client.orgs().list(query));
    }

    public CompletableFuture<Map<String, Object>> create(Map<String, Object> body) {
      return supply(() -> client.orgs().create(body));
    }

    public CompletableFuture<Map<String, Object>> get(String orgId) {
      return supply(() -> client.orgs().get(orgId));
    }

    public CompletableFuture<Map<String, Object>> update(String orgId, Map<String, Object> body) {
      return supply(() -> client.orgs().update(orgId, body));
    }

    public CompletableFuture<Map<String, Object>> usage(String parentOrgId) {
      return supply(() -> client.orgs().usage(parentOrgId));
    }

    public CompletableFuture<Map<String, Object>> purgeCalls(
        String orgId, boolean includeSubOrgs) {
      return supply(() -> client.orgs().purgeCalls(orgId, includeSubOrgs));
    }

    public CompletableFuture<Map<String, Object>> listEnvs(String orgId) {
      return supply(() -> client.orgs().listEnvs(orgId));
    }

    public CompletableFuture<Map<String, Object>> createEnv(
        String orgId, Map<String, Object> body) {
      return supply(() -> client.orgs().createEnv(orgId, body));
    }

    public CompletableFuture<Map<String, Object>> getEnv(String orgId, String envId) {
      return supply(() -> client.orgs().getEnv(orgId, envId));
    }

    public CompletableFuture<Map<String, Object>> listApiKeys(
        String orgId, Map<String, String> query) {
      return supply(() -> client.orgs().listApiKeys(orgId, query));
    }

    public CompletableFuture<Map<String, Object>> createApiKey(
        String orgId, Map<String, Object> body) {
      return supply(() -> client.orgs().createApiKey(orgId, body));
    }

    public CompletableFuture<Map<String, Object>> listEnvApiKeys(
        String orgId, String envId, Map<String, String> query) {
      return supply(() -> client.orgs().listEnvApiKeys(orgId, envId, query));
    }

    public CompletableFuture<Map<String, Object>> createEnvApiKey(
        String orgId, String envId, Map<String, Object> body) {
      return supply(() -> client.orgs().createEnvApiKey(orgId, envId, body));
    }

    /** Async view of {@link OrgsResource#env}: projects, builds and context in one environment. */
    public OrgEnv env(String orgId, String envId) {
      return new OrgEnv(client.orgs().env(orgId, envId));
    }
  }

  /** Async view of {@link dev.hot.sdk.OrgEnv}. */
  public final class OrgEnv {
    private final dev.hot.sdk.OrgEnv env;
    private final Projects projects;
    private final Builds builds;
    private final Context context;

    private OrgEnv(dev.hot.sdk.OrgEnv env) {
      this.env = env;
      this.projects = new Projects(env.projects());
      this.builds = new Builds(env.builds());
      this.context = new Context(env.context());
    }

    /** Async projects resource scoped to this environment. */
    public Projects projects() {
      return projects;
    }

    /** Async builds resource scoped to this environment. */
    public Builds builds() {
      return builds;
    }

    /** Async context resource scoped to this environment. */
    public Context context() {
      return context;
    }

    public CompletableFuture<Map<String, Object>> listProjects(Map<String, String> query) {
      return projects.list(query);
    }

    public CompletableFuture<Map<String, Object>> createProject(Map<String, Object> body) {
      return projects.create(body);
    }

    public CompletableFuture<Map<String, Object>> getProject(String project) {
      return projects.get(project);
    }

    public CompletableFuture<Map<String, Object>> updateProject(
        String project, Map<String, Object> body) {
      return projects.update(project, body);
    }

    public CompletableFuture<Void> deleteProject(String project) {
      return projects.delete(project);
    }

    public CompletableFuture<Map<String, Object>> activateProject(String project) {
      return projects.activate(project);
    }

    public CompletableFuture<Map<String, Object>> deactivateProject(String project) {
      return projects.deactivate(project);
    }

    public CompletableFuture<Map<String, Object>> projectEventHandlers(String project) {
      return projects.eventHandlers(project);
    }

    public CompletableFuture<Map<String, Object>> projectSchedules(String project) {
      return projects.schedules(project);
    }

    public CompletableFuture<Map<String, Object>> listBuilds(Map<String, String> query) {
      return builds.list(query);
    }

    public CompletableFuture<Map<String, Object>> listProjectBuilds(
        String project, Map<String, String> query) {
      return builds.listForProject(project, query);
    }

    public CompletableFuture<Map<String, Object>> getBuild(String project, String buildId) {
      return builds.get(project, buildId);
    }

    public CompletableFuture<Map<String, Object>> deployedBuild(String project) {
      return builds.deployed(project);
    }

    public CompletableFuture<Map<String, Object>> liveBuild(String project) {
      return builds.live(project);
    }

    public CompletableFuture<Map<String, Object>> uploadBuild(String project, BuildUpload upload) {
      return builds.upload(project, upload);
    }

    public CompletableFuture<HttpResponse<InputStream>> downloadBuild(
        String project, String buildId) {
      return builds.download(project, buildId);
    }

    public CompletableFuture<Map<String, Object>> deployBuild(String project, String buildId) {
      return builds.deploy(project, buildId);
    }

    public CompletableFuture<Map<String, Object>> listContext(String project) {
      return context.list(project);
    }

    public CompletableFuture<Map<String, Object>> createContext(
        String project, Map<String, Object> body) {
      return context.create(project, body);
    }

    public CompletableFuture<Map<String, Object>> updateContext(
        String project, String key, Map<String, Object> body) {
      return context.update(project, key, body);
    }

    public CompletableFuture<Void> deleteContext(String project, String key) {
      return context.delete(project, key);
    }

    /** The synchronous resources, for calls without an async helper here. */
    public dev.hot.sdk.OrgEnv sync() {
      return env;
    }
  }

  /** Async view of {@link ApiKeysResource}. */
  public final class ApiKeys {
    public CompletableFuture<Map<String, Object>> get(String keyId) {
      return supply(() -> client.apiKeys().get(keyId));
    }

    public CompletableFuture<Map<String, Object>> update(String keyId, Map<String, Object> body) {
      return supply(() -> client.apiKeys().update(keyId, body));
    }

    public CompletableFuture<Void> revoke(String keyId) {
      return run(() -> client.apiKeys().revoke(keyId));
    }

    public CompletableFuture<Map<String, Object>> rotate(String keyId, Map<String, Object> body) {
      return supply(() -> client.apiKeys().rotate(keyId, body));
    }
  }
}
