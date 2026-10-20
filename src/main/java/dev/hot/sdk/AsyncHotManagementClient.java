package dev.hot.sdk;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/** Async wrapper over the separate root management client. */
public final class AsyncHotManagementClient {
  private final HotManagementClient client;
  private final Executor executor;

  private AsyncHotManagementClient(HotManagementClient client, Executor executor) {
    this.client = client;
    this.executor = executor;
  }

  public static AsyncHotManagementClient wrap(HotManagementClient client) {
    return wrap(client, ForkJoinPool.commonPool());
  }

  public static AsyncHotManagementClient wrap(HotManagementClient client, Executor executor) {
    return new AsyncHotManagementClient(client, executor);
  }

  public CompletableFuture<Map<String, Object>> listChildren(Map<String, String> query) {
    return CompletableFuture.supplyAsync(() -> client.listChildren(query), executor);
  }

  public CompletableFuture<Map<String, Object>> createChild(Map<String, Object> body) {
    return CompletableFuture.supplyAsync(() -> client.createChild(body), executor);
  }

  public CompletableFuture<Void> setChildState(String childId, String state) {
    return CompletableFuture.runAsync(() -> client.setChildState(childId, state), executor);
  }

  public CompletableFuture<Map<String, Object>> createEnvironment(
      String childId, Map<String, Object> body) {
    return CompletableFuture.supplyAsync(() -> client.createEnvironment(childId, body), executor);
  }

  public CompletableFuture<Map<String, Object>> createApiKey(
      String childId, String envId, Map<String, Object> body) {
    return CompletableFuture.supplyAsync(() -> client.createApiKey(childId, envId, body), executor);
  }

  public CompletableFuture<Map<String, Object>> usage() {
    return CompletableFuture.supplyAsync(() -> client.usage(), executor);
  }
}
