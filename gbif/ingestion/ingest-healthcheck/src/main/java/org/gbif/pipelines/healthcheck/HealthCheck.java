package org.gbif.pipelines.healthcheck;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Health check utility that monitors a RabbitMQ queue and a Prometheus metric to determine service
 * health. This has been written to be used in Kubernetes liveness/readiness probes. The service is
 * considered unhealthy if there are messages in the RabbitMQ queue that have not been consumed for
 * a specified threshold duration.
 */
public final class HealthCheck {

  private static final String RABBITMQ_URL = requireEnv("RABBITMQ_URL");
  private static final String RABBITMQ_USER = requireEnv("RABBITMQ_USER");
  private static final String RABBITMQ_PASSWORD = requireEnv("RABBITMQ_PASSWORD");
  private static final String PROMETHEUS_URL =
      System.getenv().getOrDefault("PROMETHEUS_URL", "http://localhost:9404");
  private static final int STALE_THRESHOLD_SECONDS =
      Integer.parseInt(System.getenv().getOrDefault("STALE_THRESHOLD_SECONDS", "1800"));
  private static final int CHECK_INTERVAL_SECONDS =
      Integer.parseInt(System.getenv().getOrDefault("CHECK_INTERVAL_SECONDS", "5"));
  private static final int PORT = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));
  public static final String LAST_CONSUMED_DATASET_TIMESTAMP_SECONDS =
      System.getenv()
          .getOrDefault(
              "PROMETHEUS_TIMESTAMP_TO_CHECK", "last_consumed_dataset_timestamp_milliseconds");

  private static volatile boolean healthy = true;
  private static volatile String debug = "NOT_SET";

  private static final HttpClient httpClient =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

  public static void main(String[] args) throws Exception {
    System.out.println("Starting health checker");
    startHealthCheckLoop();
    startHttpServer();
  }

  private static void startHealthCheckLoop() {
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    scheduler.scheduleAtFixedRate(
        () -> {
          try {
            doHeathCheck();
          } catch (Exception e) {
            healthy = false;
          }
        },
        0,
        CHECK_INTERVAL_SECONDS,
        TimeUnit.SECONDS);
  }

  /**
   * Performs a single health check by querying RabbitMQ and Prometheus.
   *
   * @throws Exception
   */
  private static void doHeathCheck() throws Exception {
    try {
      int messagesReady = fetchRabbitMqMessagesReady();
      long lastConsumedTimestamp = fetchPrometheusTimestamp();
      long ageSeconds = Instant.now().getEpochSecond() - lastConsumedTimestamp;
      System.out.printf(
          "Queued messages_ready=%d, last consumed age=%d seconds%n", messagesReady, ageSeconds);
      healthy = !(messagesReady > 0 && ageSeconds > STALE_THRESHOLD_SECONDS);
      debug =
          String.format(
              "Queued messages_ready=%d, last consumed age=%d seconds%n",
              messagesReady, ageSeconds);
    } catch (Exception e) {
      debug = "Exception during health check: " + e.getMessage();
      throw e;
    }
  }

  /* =======================
  RabbitMQ
  ======================= */

  private static int fetchRabbitMqMessagesReady() throws Exception {
    String auth =
        Base64.getEncoder()
            .encodeToString(
                (RABBITMQ_USER + ":" + RABBITMQ_PASSWORD).getBytes(StandardCharsets.UTF_8));

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(RABBITMQ_URL))
            .timeout(Duration.ofSeconds(3))
            .header("Authorization", "Basic " + auth)
            .header("Accept", "application/json")
            .GET()
            .build();

    HttpResponse<InputStream> response =
        httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());

    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      System.err.println("RabbitMQ status code" + response.statusCode());
      throw new IOException("RabbitMQ non-2xx response");
    }

    String body = new String(response.body().readAllBytes());
    return parseJsonInt(body, "messages_ready");
  }

  /* =======================
  Prometheus
  ======================= */

  private static long fetchPrometheusTimestamp() throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(PROMETHEUS_URL))
            .timeout(Duration.ofSeconds(3))
            .GET()
            .build();

    HttpResponse<InputStream> response =
        httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());

    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      System.err.println("Prometheus URL status code" + response.statusCode());
      throw new IOException("Prometheus non-2xx response");
    }

    String body = new String(response.body().readAllBytes());
    return parsePrometheusLong(body, LAST_CONSUMED_DATASET_TIMESTAMP_SECONDS);
  }

  /* =======================
  Parsing helpers
  ======================= */

  private static int parseJsonInt(String json, String field) {
    String key = "\"" + field + "\"";
    int idx = json.indexOf(key);
    if (idx < 0) {
      throw new IllegalArgumentException("Missing field: " + field);
    }

    int colon = json.indexOf(':', idx);
    int i = colon + 1;
    while (Character.isWhitespace(json.charAt(i))) i++;

    int start = i;
    while (Character.isDigit(json.charAt(i))) i++;

    return Integer.parseInt(json.substring(start, i));
  }

  private static long parsePrometheusLong(String metrics, String metricName) {
    for (String line : metrics.split("\n")) {
      if (line.startsWith(metricName + " ")) {
        String[] parts = line.split("\\s+");
        double timeDouble = Double.parseDouble(parts[1]);
        return (long) timeDouble;
      }
    }
    throw new IllegalArgumentException("Missing Prometheus metric: " + metricName);
  }

  /* =======================
  HTTP server
  ======================= */

  private static void startHttpServer() throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", PORT), 0);
    server.createContext("/", new HealthHandler());
    server.createContext("/debug", new DebugHealthHandler());
    server.setExecutor(Executors.newSingleThreadExecutor());
    server.start();
  }

  private static final class HealthHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      if (!"GET".equals(exchange.getRequestMethod())) {
        exchange.sendResponseHeaders(405, -1);
        return;
      }

      int status = healthy ? 200 : 500;
      byte[] body = healthy ? "OK\n".getBytes() : "UNHEALTHY\n".getBytes();

      exchange.sendResponseHeaders(status, body.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(body);
      }
    }
  }

  private static final class DebugHealthHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      if (!"GET".equals(exchange.getRequestMethod())) {
        exchange.sendResponseHeaders(405, -1);
        return;
      }
      byte[] body = debug.getBytes();
      exchange.sendResponseHeaders(200, body.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(body);
      }
    }
  }

  private static String requireEnv(String name) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("Missing required env var: " + name);
    }
    return value;
  }
}
