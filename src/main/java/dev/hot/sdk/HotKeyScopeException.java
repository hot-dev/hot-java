package dev.hot.sdk;

import java.util.Map;

/**
 * Thrown for {@code 403 key_scope_mismatch}: the call needs a different kind of API key. Org API
 * keys call only {@code /v1/orgs} and {@code /v1/api-keys} ({@link HotClient#orgs()},
 * {@link HotClient#apiKeys()}); environment API keys, sessions and service keys call every other
 * (runtime) route. Use one client per credential.
 */
public final class HotKeyScopeException extends HotApiException {

  static final String CODE = "key_scope_mismatch";

  private final String requiredScope;

  public HotKeyScopeException(
      int statusCode,
      String message,
      String requestId,
      Map<String, Object> errorBody,
      String requiredScope) {
    super(statusCode, describe(message, requiredScope), CODE, requestId, null, errorBody);
    this.requiredScope = requiredScope;
  }

  /**
   * The key scope the call needs: {@code "org"} for org API routes, {@code "env"} for runtime
   * routes.
   */
  public String requiredScope() {
    return requiredScope;
  }

  static String requiredScope(String path, String message) {
    if (path != null) {
      return path.startsWith("/orgs") || path.startsWith("/api-keys") ? "org" : "env";
    }
    return message != null && message.contains("needs an org API key") ? "org" : "env";
  }

  // The server's message already names the key kind; keep it when it does.
  private static String describe(String message, String requiredScope) {
    if (message != null && message.contains("needs an")) {
      return message;
    }
    String needed =
        requiredScope.equals("org")
            ? "This call needs an org API key"
            : "This call needs an environment API key, session or service key";
    return needed + ": " + message;
  }
}
