package org.gbif.pipelines.interpretation.standalone;

import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.IOException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.ConnectionParameters;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.pipelines.core.config.model.MessagingConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.jetbrains.annotations.NotNull;

/** The runnable */
@Slf4j
public class Standalone {

  private volatile boolean running = true;

  @Parameters(separators = "=")
  private static class Args {

    @Parameter(names = "--mode", description = "mode", required = true)
    private String mode;

    @Parameter(
        names = "--config",
        description = "Path to YAML configuration file",
        required = false)
    private String config = "/tmp/pipelines-spark.yaml";

    @Parameter(names = "--threads", description = "Number of threads", required = false)
    private int threads = 1;

    @Parameter(names = "--queueName", description = "queueName", required = true)
    private String queueName;

    @Parameter(names = "--routingKey", description = "routingKey", required = true)
    private String routingKey;

    @Parameter(names = "--exchange", description = "exchange", required = true)
    private String exchange = "occurrence";
  }

  public static void main(String[] argsv) throws Exception {

    Standalone.Args args = new Standalone.Args();
    JCommander jCommander = new JCommander(args);
    jCommander.parse(argsv);
    Mode mode = Mode.valueOf(args.mode);
    PipelinesConfig config = loadConfig(args.config);

    new Standalone()
        .start(mode, config, args.queueName, args.routingKey, args.exchange, args.threads);
  }

  public void start(
      Mode mode,
      PipelinesConfig config,
      String queueName,
      String routingKey,
      String exchange,
      int threads) {

    Function<MessagePublisher, PipelinesCallback> callbackFn = null;

    switch (mode) {
      case IDENTIFIER:
        callbackFn = (messagePublisher -> new IdentifierCallback(config, messagePublisher));
        break;
      case IDENTIFIER_DISTRIBUTED:
        callbackFn =
            (messagePublisher -> new IdentifierDistributedCallback(config, messagePublisher));
        break;
      case INTERPRETATION:
        callbackFn = (messagePublisher -> new InterpretationCallback(config, messagePublisher));
        break;
      case INTERPRETATION_DISTRIBUTED:
        callbackFn =
            (messagePublisher -> new InterpretationDistributedCallback(config, messagePublisher));
        break;
      case TABLEBUILD:
        callbackFn = (messagePublisher -> new TableBuildCallback(config, messagePublisher));
        break;
      case TABLEBUILD_DISTRIBUTED:
        callbackFn =
            (messagePublisher -> new TableBuildDistributedCallback(config, messagePublisher));
        break;
      case INDEXING:
        callbackFn = (messagePublisher -> new IndexingCallback(config, messagePublisher));
        break;
      case INDEXING_DISTRIBUTED:
        callbackFn =
            (messagePublisher -> new IndexingDistributedCallback(config, messagePublisher));
        break;
      case FRAGMENTER:
        callbackFn = (messagePublisher -> new FragmenterCallback(config, messagePublisher));
        break;
      case FRAGMENTER_DISTRIBUTED:
        callbackFn =
            (messagePublisher -> new FragmenterDistributedCallback(config, messagePublisher));
        break;
      default:
        throw new IllegalArgumentException(
            "Unknown mode: "
                + mode
                + ". Recognized modes are: "
                + Stream.of(Mode.values()).map(Enum::name).collect(Collectors.joining(",")));
    }

    log.info(
        "Running {}, listening to queue: {} on virtual host {}",
        mode,
        queueName,
        config.getStandalone().getMessaging().getVirtualHost());
    setupShutdown();

    try (MessageListener listener = createListener(config);
        DefaultMessagePublisher publisher = createPublisher(config);
        PipelinesCallback callback = callbackFn.apply(publisher)) {

      // initialise spark session & filesystem
      callback.init();

      // start the listener
      listener.listen(queueName, routingKey, exchange, threads, callback);

      // 5. Keep running until shutdown
      while (running) {
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }

    } catch (IOException e) {
      log.error("Error starting standalone", e);
    }

    log.info("Exiting Standalone.");
  }

  @NotNull
  private static MessageListener createListener(PipelinesConfig pipelinesConfig)
      throws IOException {
    MessagingConfig messagingConfig = pipelinesConfig.getStandalone().getMessaging();
    return new MessageListener(
        new ConnectionParameters(
            messagingConfig.getHost(),
            messagingConfig.getPort(),
            messagingConfig.getUsername(),
            messagingConfig.getPassword(),
            messagingConfig.getVirtualHost()),
        messagingConfig.getPrefetchCount());
  }

  @NotNull
  private static DefaultMessagePublisher createPublisher(PipelinesConfig pipelinesConfig)
      throws IOException {
    MessagingConfig messagingConfig = pipelinesConfig.getStandalone().getMessaging();
    return new DefaultMessagePublisher(
        new ConnectionParameters(
            messagingConfig.getHost(),
            messagingConfig.getPort(),
            messagingConfig.getUsername(),
            messagingConfig.getPassword(),
            messagingConfig.getVirtualHost()));
  }

  private void setupShutdown() {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  log.info("Shutdown signal received. Cleaning up...");
                  running = false;
                  log.info("Graceful shutdown complete.");
                }));
  }

  public enum Mode {
    IDENTIFIER,
    IDENTIFIER_DISTRIBUTED,
    INTERPRETATION,
    INTERPRETATION_DISTRIBUTED,
    TABLEBUILD,
    TABLEBUILD_DISTRIBUTED,
    INDEXING,
    INDEXING_DISTRIBUTED,
    FRAGMENTER,
    FRAGMENTER_DISTRIBUTED
  }
}
