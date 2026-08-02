package dev.hot.sdk;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Asynchronous Hot API v1 client returning {@link CompletableFuture}s. A thin wrapper over
 * {@link HotClient} that runs each call on the configured executor (default
 * {@link ForkJoinPool#commonPool()}); streaming methods drive the event iterator on that
 * executor and deliver events to a consumer.
 */
public final class AsyncHotClient {

  private final HotClient client;
  private final Executor executor;
  private final Events events;
  private final Runs runs;
  private final Env env;
  private final Org org;
  private final Streams streams;
  private final Tasks tasks;
  private final Files files;
  private final Projects projects;
  private final Builds builds;
  private final Context context;
  private final Domains domains;
  private final Sessions sessions;
  private final ServiceKeys serviceKeys;

  private AsyncHotClient(HotClient client, Executor executor) {
    this.client = client;
    this.executor = executor;
    this.events = new Events();
    this.runs = new Runs();
    this.env = new Env();
    this.org = new Org();
    this.streams = new Streams();
    this.tasks = new Tasks();
    this.files = new Files();
    this.projects = new Projects();
    this.builds = new Builds();
    this.context = new Context();
    this.domains = new Domains();
    this.sessions = new Sessions();
    this.serviceKeys = new ServiceKeys();
  }

  /** Wraps a synchronous client with the common pool. */
  public static AsyncHotClient wrap(HotClient client) {
    return wrap(client, ForkJoinPool.commonPool());
  }

  /** Wraps a synchronous client with an explicit executor. */
  public static AsyncHotClient wrap(HotClient client, Executor executor) {
    return new AsyncHotClient(client, executor);
  }

  /** The underlying synchronous client. */
  public HotClient sync() {
    return client;
  }

  private <T> CompletableFuture<T> supply(Supplier<T> supplier) {
    return CompletableFuture.supplyAsync(supplier, executor);
  }

  private CompletableFuture<Void> drain(StreamEvents source, Consumer<Map<String, Object>> onEvent) {
    return CompletableFuture.runAsync(
        () -> {
          try (source) {
            while (source.hasNext()) {
              onEvent.accept(source.next());
            }
          }
        },
        executor);
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

  /** Async escape hatch mirroring {@link HotClient#request}. */
  public CompletableFuture<Map<String, Object>> request(
      String method, String path, Object body, Map<String, String> query) {
    return supply(() -> client.request(method, path, body, query));
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
      return supply(() -> client.runs().subscribe(runId))
          .thenCompose(source -> drain(source, onEvent));
    }
  }

  /** Async view of {@link EnvResource}. */
  public final class Env {
    public CompletableFuture<Map<String, Object>> get() {
      return supply(() -> client.env().get());
    }

    /** Streams environment events to {@code onEvent} until the subscription ends. */
    public CompletableFuture<Void> subscribe(Consumer<Map<String, Object>> onEvent) {
      return supply(() -> client.env().subscribe()).thenCompose(source -> drain(source, onEvent));
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
      return supply(() -> client.streams().subscribe(streamId, project))
          .thenCompose(source -> drain(source, onEvent));
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
      return supply(() -> client.streams().subscribeWithEvent(body, options))
          .thenCompose(source -> drain(source, onEvent));
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
      return supply(() -> client.tasks().subscribe(taskId))
          .thenCompose(source -> drain(source, onEvent));
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
      return CompletableFuture.runAsync(() -> client.files().delete(fileId), executor);
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
      return CompletableFuture.runAsync(() -> client.files().abortUpload(uploadId), executor);
    }
  }

  /** Async view of {@link ProjectsResource}. */
  public final class Projects {
    public CompletableFuture<Map<String, Object>> list(Map<String, String> query) {
      return supply(() -> client.projects().list(query));
    }

    public CompletableFuture<Map<String, Object>> create(Map<String, Object> body) {
      return supply(() -> client.projects().create(body));
    }

    public CompletableFuture<Map<String, Object>> get(String project) {
      return supply(() -> client.projects().get(project));
    }

    public CompletableFuture<Map<String, Object>> update(String project, Map<String, Object> body) {
      return supply(() -> client.projects().update(project, body));
    }

    public CompletableFuture<Void> delete(String project) {
      return CompletableFuture.runAsync(() -> client.projects().delete(project), executor);
    }

    public CompletableFuture<Map<String, Object>> activate(String project) {
      return supply(() -> client.projects().activate(project));
    }

    public CompletableFuture<Map<String, Object>> deactivate(String project) {
      return supply(() -> client.projects().deactivate(project));
    }

    public CompletableFuture<Map<String, Object>> eventHandlers(String project) {
      return supply(() -> client.projects().eventHandlers(project));
    }

    public CompletableFuture<Map<String, Object>> schedules(String project) {
      return supply(() -> client.projects().schedules(project));
    }
  }

  /** Async view of {@link BuildsResource}. */
  public final class Builds {
    public CompletableFuture<Map<String, Object>> list(Map<String, String> query) {
      return supply(() -> client.builds().list(query));
    }

    public CompletableFuture<Map<String, Object>> listForProject(
        String project, Map<String, String> query) {
      return supply(() -> client.builds().listForProject(project, query));
    }

    public CompletableFuture<Map<String, Object>> get(String project, String buildId) {
      return supply(() -> client.builds().get(project, buildId));
    }

    public CompletableFuture<Map<String, Object>> deployed(String project) {
      return supply(() -> client.builds().deployed(project));
    }

    public CompletableFuture<Map<String, Object>> live(String project) {
      return supply(() -> client.builds().live(project));
    }

    public CompletableFuture<Map<String, Object>> upload(String project, BuildUpload upload) {
      return supply(() -> client.builds().upload(project, upload));
    }

    public CompletableFuture<Map<String, Object>> deploy(String project, String buildId) {
      return supply(() -> client.builds().deploy(project, buildId));
    }
  }

  /** Async view of {@link ContextResource}. */
  public final class Context {
    public CompletableFuture<Map<String, Object>> list(String project) {
      return supply(() -> client.context().list(project));
    }

    public CompletableFuture<Map<String, Object>> create(String project, Map<String, Object> body) {
      return supply(() -> client.context().create(project, body));
    }

    public CompletableFuture<Map<String, Object>> update(
        String project, String key, Map<String, Object> body) {
      return supply(() -> client.context().update(project, key, body));
    }

    public CompletableFuture<Void> delete(String project, String key) {
      return CompletableFuture.runAsync(() -> client.context().delete(project, key), executor);
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
      return CompletableFuture.runAsync(() -> client.domains().delete(domainId), executor);
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
      return CompletableFuture.runAsync(() -> client.sessions().revoke(sessionId), executor);
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
      return CompletableFuture.runAsync(() -> client.serviceKeys().revoke(serviceKeyId), executor);
    }

    public CompletableFuture<Map<String, Object>> revokeAll() {
      return supply(() -> client.serviceKeys().revokeAll());
    }
  }
}
