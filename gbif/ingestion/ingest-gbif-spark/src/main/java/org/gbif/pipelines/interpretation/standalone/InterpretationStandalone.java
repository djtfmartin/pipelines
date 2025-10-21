package org.gbif.pipelines.interpretation.standalone;

import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;

import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.core.config.model.MessagingConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

@Slf4j
public class InterpretationStandalone {

    private volatile boolean running = true;

    public static void main(String[] args) {

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

    public void start(PipelinesConfig pipelinesConfig) {

        MessagingConfig messagingConfig = pipelinesConfig.getStandalone().getMessaging();
        String queueName = messagingConfig.getQueueName();
        String host = messagingConfig.getHost();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // 1. Declare the queue (idempotent)
            channel.queueDeclare(queueName, true, false, false, null);
            System.out.println("Listening for messages on RabbitMQ queue: " + queueName);

            // 2. Setup graceful shutdown hook
            Runtime.getRuntime().addShutdownHook(
                    new Thread(() -> {
                        System.out.println("\nShutdown signal received. Cleaning up...");
                        running = false;
                        try {
                            channel.close();
                            connection.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        System.out.println("Graceful shutdown complete.");
                    })
            );

            // 3. Consumer callback
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.println("Received: " + message);
            };

            CancelCallback cancelCallback = consumerTag ->
                    System.out.println("Consumer " + consumerTag + " canceled");

            // 4. Start consuming
            channel.basicConsume(queueName, true, deliverCallback, cancelCallback);

            // 5. Keep running until shutdown
            while (running) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            System.out.println("Listener stopped.");

        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }
}
