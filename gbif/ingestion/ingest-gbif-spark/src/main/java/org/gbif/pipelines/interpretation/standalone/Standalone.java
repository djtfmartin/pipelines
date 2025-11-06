package org.gbif.pipelines.interpretation.standalone;

import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;

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

  public static void main(String[] args) throws Exception {

    if (args.length >= 2) {
      throw new IllegalArgumentException("Expecting two arguments: <mode> <config-file> <threads>");
    }

    Mode mode = Mode.valueOf(args[0].toUpperCase());
    PipelinesConfig config = loadConfig(args[1]);
    int threads = 1;
    if (args.length == 3) {
      threads = Integer.parseInt(args[2]);
    }

    switch (mode) {
      case IDENTIFIER:
        new Standalone()
            .start(
                mode,
                config,
                "pipelines_occurrence_identifier_standalone",
                "occurrence.pipelines.verbatim.finished.identifier",
                "occurrence",
                threads,
                (messagePublisher -> new IdentifierCallback(config, messagePublisher)));
        break;
      case INTERPRETATION:
        new Standalone()
            .start(
                mode,
                config,
                "pipelines_occurrence_interpretation_standalone", // FIXME used ?
                // exchange/routingkey are used
                "occurrence.pipelines.verbatim.finished",
                "occurrence",
                threads,
                (messagePublisher -> new InterpretationCallback(config, messagePublisher)));
        break;
      case TABLEBUILD:
        new Standalone()
            .start(
                mode,
                config,
                "pipelines_occurrence_hdfs_view_standalone",
                "occurrence.pipelines.interpretation.finished",
                "occurrence",
                threads,
                (messagePublisher -> new TableBuildCallback(config, messagePublisher)));
        break;
      case INDEXING:
        new Standalone()
            .start(
                mode,
                config,
                "pipelines_occurrence_indexing_standalone",
                "occurrence.pipelines.interpretation.finished",
                "occurrence",
                threads,
                (messagePublisher -> new IndexingCallback(config, messagePublisher)));
        break;
      case FRAGMENTER:
        new Standalone()
            .start(
                mode,
                config,
                "pipelines_occurrence_fragmenter_standalone",
                "occurrence.pipelines.interpretation.finished" + ".*",
                "occurrence",
                threads,
                (messagePublisher -> new FragmenterCallback(config, messagePublisher)));
        break;
      default:
        throw new IllegalArgumentException(
            "Unknown mode: "
                + args[0]
                + ". Recognized modes are: "
                + Stream.of(Mode.values()).map(Enum::name).collect(Collectors.joining(",")));
    }
  }

  public void start(
      Mode mode,
      PipelinesConfig pipelinesConfig,
      String queueName,
      String routingKey,
      String exchange,
      int threads,
      Function<MessagePublisher, PipelinesCallback> callbackCreateFcn) {

    log.info("Starting standalone mode: {}, listening to queue: {}", mode, queueName);
    setupShutdown();

    try (MessageListener listener = createListener(pipelinesConfig);
        DefaultMessagePublisher publisher = createPublisher(pipelinesConfig);
        PipelinesCallback callback = callbackCreateFcn.apply(publisher)) {

      // initialise spark session & filesystem
      callback.init();

      // start the listener
      listener.listen(
          queueName, // is this used ? - exchange / routing key are used
          routingKey,
          exchange,
          threads,
          callback);

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
    INTERPRETATION,
    IDENTIFIER,
    TABLEBUILD,
    INDEXING,
    FRAGMENTER
  }
}
