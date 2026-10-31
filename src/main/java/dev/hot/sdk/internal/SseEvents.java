package dev.hot.sdk.internal;

import dev.hot.sdk.HotTransportException;
import dev.hot.sdk.StreamEvents;
import java.io.UncheckedIOException;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/** Decodes an SSE response body into stream events. Not part of the public API. */
public final class SseEvents implements StreamEvents {

  // Closes streams that stop receiving lines (keepalive comments included), so a dead socket
  // surfaces as a transport error instead of blocking forever.
  private static final ScheduledExecutorService IDLE_WATCHDOG =
      Executors.newSingleThreadScheduledExecutor(
          runnable -> {
            Thread thread = new Thread(runnable, "hot-sdk-sse-idle-watchdog");
            thread.setDaemon(true);
            return thread;
          });

  private final Stream<String> lines;
  private final Iterator<String> iterator;
  private final boolean bigDecimals;
  private final long idleNanos;
  private final ScheduledFuture<?> idleCheck;
  private Map<String, Object> next;
  // volatile: close() may arrive from a watchdog thread (see RunWaiter).
  private volatile boolean closed;
  private volatile boolean idleTimedOut;
  private volatile long lastLine = System.nanoTime();
  private boolean sawComplete;

  SseEvents(HttpResponse<Stream<String>> response) {
    this(response, false, null);
  }

  SseEvents(HttpResponse<Stream<String>> response, boolean bigDecimals, Duration idleTimeout) {
    this.lines = response.body();
    this.iterator = lines.iterator();
    this.bigDecimals = bigDecimals;
    if (idleTimeout != null) {
      this.idleNanos = idleTimeout.toNanos();
      long period = Math.max(1, idleTimeout.toMillis() / 3);
      this.idleCheck =
          IDLE_WATCHDOG.scheduleAtFixedRate(this::checkIdle, period, period, TimeUnit.MILLISECONDS);
    } else {
      this.idleNanos = 0;
      this.idleCheck = null;
    }
  }

  @Override
  public boolean hasNext() {
    if (next != null) {
      return true;
    }
    if (closed) {
      failIfIdle();
      return false;
    }
    advance();
    return next != null;
  }

  @Override
  public Map<String, Object> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Map<String, Object> event = next;
    next = null;
    return event;
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      if (idleCheck != null) {
        idleCheck.cancel(false);
      }
      lines.close();
    }
  }

  /** Whether this connection delivered the server's stream:complete (its 5-minute rollover). */
  public boolean sawComplete() {
    return sawComplete;
  }

  private void checkIdle() {
    if (!closed && System.nanoTime() - lastLine > idleNanos) {
      idleTimedOut = true;
      close();
    }
  }

  private void failIfIdle() {
    if (idleTimedOut) {
      idleTimedOut = false;
      throw new HotTransportException(
          "SSE stream received nothing for " + TimeUnit.NANOSECONDS.toSeconds(idleNanos) + " s",
          null);
    }
  }

  private void advance() {
    StringBuilder data = new StringBuilder();
    boolean hasData = false;
    try {
      while (iterator.hasNext()) {
        String line = iterator.next();
        lastLine = System.nanoTime();
        if (line.isEmpty()) {
          if (hasData) {
            Map<String, Object> event = parse(data.toString());
            data.setLength(0);
            hasData = false;
            if (event != null) {
              next = event;
              return;
            }
          }
          continue;
        }
        if (line.startsWith("data: ")) {
          data.append(line.substring(6));
          hasData = true;
        } else if (line.startsWith("data:")) {
          data.append(line.substring(5));
          hasData = true;
        }
      }
    } catch (UncheckedIOException error) {
      close();
      failIfIdle();
      throw new HotTransportException(error.getCause());
    } catch (RuntimeException error) {
      // A close from another thread can surface as an unchecked cancellation.
      if (!closed) {
        throw error;
      }
    }

    // End of stream: flush a trailing block that lacked its final blank line.
    if (hasData) {
      next = parse(data.toString());
    }
    close();
    if (next == null) {
      failIfIdle();
    }
  }

  private Map<String, Object> parse(String payload) {
    Map<String, Object> parsed = Json.tryReadObject(payload, bigDecimals);
    if (parsed != null && parsed.get("type") instanceof String type) {
      if (type.equals("stream:complete")) {
        sawComplete = true;
      }
      return parsed;
    }
    return null;
  }
}
