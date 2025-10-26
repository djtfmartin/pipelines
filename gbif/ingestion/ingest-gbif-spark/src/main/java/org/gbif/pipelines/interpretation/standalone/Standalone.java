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
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.core.config.model.MessagingConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.jetbrains.annotations.NotNull;

/** The runnable */
@Slf4j
public class Standalone {

  private volatile boolean running = true;

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      throw new IllegalArgumentException("Expecting two arguments: <mode> <config-file>");
    }

    Mode mode = Mode.valueOf(args[0].toUpperCase());
    PipelinesConfig config = loadConfig(args[1]);
    switch (mode) {
      case IDENTIFIER:
        new Standalone()
            .start(
                mode,
                config,
                "pipelines_occurrence_identifier_standalone",
                "occurrence.pipelines.verbatim.finished.identifier",
                "occurrence",
                1,
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
                1,
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
                1,
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
                1,
                (messagePublisher -> new IndexingCallback(config, messagePublisher)));
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
      Function<MessagePublisher, MessageCallback<PipelinesVerbatimMessage>> callbackCreateFcn) {

    System.out.println("Starting standalone mode: " + mode);
    System.out.println("Starting standalone listening to queue " + queueName);

    try (MessageListener listener = createListener(pipelinesConfig);
        DefaultMessagePublisher publisher = createPublisher(pipelinesConfig)) {

      listener.listen(
          queueName, // is this used ? - exchange / routing key are used
          routingKey,
          exchange,
          threads,
          callbackCreateFcn.apply(publisher));

      setupShutdown();

      // 5. Keep running until shutdown
      while (running) {
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }

      log.info("Listener stopped.");

      System.out.println("Exiting Standalone...");

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @NotNull
  private static MessageListener createListener(PipelinesConfig pipelinesConfig)
      throws IOException {
    MessagingConfig messagingConfig = pipelinesConfig.getStandalone().getMessaging();

    // String host, int port, String username, String password, String virtualHost
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
                  log.info("\nShutdown signal received. Cleaning up...");
                  running = false;
                  log.info("Graceful shutdown complete.");
                }));
  }

  public enum Mode {
    INTERPRETATION,
    IDENTIFIER,
    TABLEBUILD,
    INDEXING
  }
}
