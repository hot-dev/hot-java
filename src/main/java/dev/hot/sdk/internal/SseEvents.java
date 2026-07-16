package dev.hot.sdk.internal;

import dev.hot.sdk.HotTransportException;
import dev.hot.sdk.StreamEvents;
import java.io.UncheckedIOException;
import java.net.http.HttpResponse;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

/** Decodes an SSE response body into stream events. Not part of the public API. */
public final class SseEvents implements StreamEvents {

  private final Stream<String> lines;
  private final Iterator<String> iterator;
  private Map<String, Object> next;
  // volatile: close() may arrive from a watchdog thread (see RunWaiter).
  private volatile boolean closed;

  SseEvents(HttpResponse<Stream<String>> response) {
    this.lines = response.body();
    this.iterator = lines.iterator();
  }

  @Override
  public boolean hasNext() {
    if (next != null) {
      return true;
    }
    if (closed) {
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
      lines.close();
    }
  }

  private void advance() {
    StringBuilder data = new StringBuilder();
    boolean hasData = false;
    try {
      while (iterator.hasNext()) {
        String line = iterator.next();
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
      throw new HotTransportException(error.getCause());
    }

    // End of stream: flush a trailing block that lacked its final blank line.
    if (hasData) {
      next = parse(data.toString());
    }
    close();
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> parse(String payload) {
    Map<String, Object> parsed = Json.tryReadObject(payload);
    if (parsed != null && parsed.get("type") instanceof String) {
      return parsed;
    }
    return null;
  }
}
