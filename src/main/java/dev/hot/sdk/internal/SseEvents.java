package dev.hot.sdk.internal;

import dev.hot.sdk.HotStreamException;
import dev.hot.sdk.HotTransportException;
import dev.hot.sdk.StreamEvents;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Decodes an SSE response body into stream events. Not part of the public API. */
public final class SseEvents implements StreamEvents {

  /** Maximum UTF-8 byte length of a single SSE event's assembled data buffer (16 MiB). */
  static final int MAX_EVENT_BYTES = 16 * 1024 * 1024;

  // Closes streams that stop receiving lines (keepalive comments included), so a dead socket
  // surfaces as a transport error instead of blocking forever.
  static final ScheduledThreadPoolExecutor IDLE_WATCHDOG = newWatchdog();

  private static ScheduledThreadPoolExecutor newWatchdog() {
    ScheduledThreadPoolExecutor executor =
        new ScheduledThreadPoolExecutor(
            1,
            runnable -> {
              Thread thread = new Thread(runnable, "hot-sdk-sse-idle-watchdog");
              thread.setDaemon(true);
              return thread;
            });
    executor.setRemoveOnCancelPolicy(true);
    return executor;
  }

  // Close the underlying stream directly so close() from a watchdog thread never deadlocks
  // against BufferedReader's internal lock held by a blocked readLine().
  private final Closeable closeTarget;
  private final BufferedReader reader;
  private final boolean bigDecimals;
  private final int maxEventBytes;
  private final long idleNanos;
  private final ScheduledFuture<?> idleCheck;
  private final StringBuilder data = new StringBuilder();
  private int dataBytes;
  private boolean hasData;
  private boolean firstLine = true;
  private Map<String, Object> next;
  // volatile: close() may arrive from a watchdog thread (see RunWaiter).
  private volatile boolean closed;
  private volatile boolean idleTimedOut;
  private volatile long lastLine = System.nanoTime();
  private boolean sawComplete;

  SseEvents(HttpResponse<InputStream> response) {
    this(response, false, null);
  }

  SseEvents(HttpResponse<InputStream> response, boolean bigDecimals, Duration idleTimeout) {
    this(response.body(), bigDecimals, idleTimeout, MAX_EVENT_BYTES);
  }

  SseEvents(InputStream stream, boolean bigDecimals, Duration idleTimeout) {
    this(stream, bigDecimals, idleTimeout, MAX_EVENT_BYTES);
  }

  SseEvents(InputStream stream, boolean bigDecimals, Duration idleTimeout, int maxEventBytes) {
    this.closeTarget = stream;
    this.reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
    this.bigDecimals = bigDecimals;
    this.maxEventBytes = maxEventBytes;
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
      try {
        closeTarget.close();
      } catch (IOException ignored) {
        // Closing an already-broken or cancelled stream is best-effort.
      }
    }
  }

  /** Whether this connection delivered the server's stream:complete (its 5-minute rollover). */
  public boolean sawComplete() {
    return sawComplete;
  }

  /**
   * Reads the next event that is already buffered without blocking on network I/O, or returns
   * {@code null} when no buffered event is available.
   */
  Map<String, Object> pollBuffered() {
    if (next != null) {
      Map<String, Object> event = next;
      next = null;
      return event;
    }
    if (closed) {
      return null;
    }
    try {
      while (!closed && reader.ready()) {
        String line = reader.readLine();
        if (line == null) {
          Map<String, Object> trailing = flushOnCleanEof();
          close();
          return trailing;
        }
        Map<String, Object> event = processLine(line);
        if (event != null) {
          return event;
        }
      }
    } catch (HotStreamException error) {
      throw error;
    } catch (IOException | RuntimeException ignored) {
      return null;
    }
    return null;
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
      long millis = TimeUnit.NANOSECONDS.toMillis(idleNanos);
      String formatted =
          millis >= 1_000 && millis % 1_000 == 0
              ? (millis / 1_000) + " s"
              : Math.max(1, millis) + " ms";
      throw new HotTransportException("SSE stream received nothing for " + formatted, null);
    }
  }

  private void advance() {
    try {
      while (!closed) {
        String line = reader.readLine();
        if (line == null) {
          break;
        }
        Map<String, Object> event = processLine(line);
        if (event != null) {
          next = event;
          return;
        }
      }
    } catch (HotStreamException error) {
      throw error;
    } catch (IOException error) {
      clearBuffer();
      close();
      failIfIdle();
      throw new HotTransportException(error);
    } catch (RuntimeException error) {
      clearBuffer();
      // A close from another thread can surface as an unchecked cancellation.
      if (!closed) {
        throw error;
      }
    }

    // End of stream: flush a trailing block only on a clean EOF, never after close() or idle.
    next = flushOnCleanEof();
    close();
    if (next == null) {
      failIfIdle();
    }
  }

  private Map<String, Object> processLine(String line) {
    lastLine = System.nanoTime();
    if (firstLine) {
      firstLine = false;
      if (!line.isEmpty() && line.charAt(0) == '\uFEFF') {
        line = line.substring(1);
      }
    }
    if (line.isEmpty()) {
      if (hasData) {
        String payload = data.toString();
        clearBuffer();
        if (!closed && !idleTimedOut) {
          return parse(payload);
        }
      }
      return null;
    }
    if (line.startsWith("data:")) {
      String value =
          line.length() > 5 && line.charAt(5) == ' ' ? line.substring(6) : line.substring(5);
      int addedBytes = utf8Length(value) + (hasData ? 1 : 0);
      if (addedBytes > maxEventBytes - dataBytes) {
        clearBuffer();
        close();
        throw new HotStreamException(
            "SSE event data exceeded maximum size of " + maxEventBytes + " bytes");
      }
      if (hasData) {
        data.append('\n');
      }
      data.append(value);
      dataBytes += addedBytes;
      hasData = true;
    }
    return null;
  }

  private Map<String, Object> flushOnCleanEof() {
    if (!hasData || closed || idleTimedOut) {
      clearBuffer();
      return null;
    }
    String payload = data.toString();
    clearBuffer();
    return parse(payload);
  }

  private void clearBuffer() {
    data.setLength(0);
    dataBytes = 0;
    hasData = false;
  }

  private static int utf8Length(String value) {
    int bytes = 0;
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c < 0x80) {
        bytes += 1;
      } else if (c < 0x800) {
        bytes += 2;
      } else if (Character.isHighSurrogate(c)
          && i + 1 < value.length()
          && Character.isLowSurrogate(value.charAt(i + 1))) {
        bytes += 4;
        i++;
      } else {
        bytes += 3;
      }
    }
    return bytes;
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
