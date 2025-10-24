package org.gbif.pipelines.interpretation.standalone;

import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;

import com.rabbitmq.client.*;
import java.io.IOException;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.ConnectionParameters;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.core.config.model.MessagingConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.jetbrains.annotations.NotNull;

@Slf4j
public class InterpretationStandalone {

  private volatile boolean running = true;

  public static void main(String[] args) throws IOException {

    if (args.length != 1) {
      throw new IllegalArgumentException(
          "Expecting a single argument to run InterpretationStandalone");
    }

    PipelinesConfig config = loadConfig(args[0]);
    MessagingConfig messagingConfig = config.getStandalone().getMessaging();

    assert messagingConfig != null;
    assert messagingConfig.getQueueName() != null;

    new InterpretationStandalone().start(config);
  }

  public void start(PipelinesConfig pipelinesConfig) throws IOException {

    System.out.println("Starting InterpretationStandalone...");

    try (MessageListener listener = createListener(pipelinesConfig);
        DefaultMessagePublisher publisher = createPublisher(pipelinesConfig)) {

      // create the callback that does everything
      InterpretationCallback interpretationCallback =
          new InterpretationCallback(pipelinesConfig, publisher);

      MessagingConfig messagingConfig = pipelinesConfig.getStandalone().getMessaging();

      listener.listen(
          messagingConfig.getQueueName(),
          PipelinesVerbatimMessage.ROUTING_KEY,
          "occurrence",
          1,
          interpretationCallback);

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

      System.out.println("Exiting InterpretationStandalone...");

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
}
