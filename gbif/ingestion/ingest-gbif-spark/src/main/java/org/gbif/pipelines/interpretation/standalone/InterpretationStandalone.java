package org.gbif.pipelines.interpretation.standalone;

import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;

import javax.jms.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.gbif.pipelines.core.config.model.MessagingConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

@Slf4j
public class InterpretationStandalone implements MessageListener {

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

    Session session = null;
    MessagingConfig messagingConfig = pipelinesConfig.getStandalone().getMessaging();
    ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(messagingConfig.getHost());

    try (Connection connection = connectionFactory.createConnection()) {

      // 1. Setup JMS connection
      connection.start();
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Destination destination = session.createQueue(messagingConfig.getQueueName());
      MessageConsumer consumer = session.createConsumer(destination);

      System.out.println("Listening for messages on queue: " + messagingConfig.getQueueName());

      // 2. Register a shutdown hook to handle SIGTERM
      Session finalSession = session;
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    System.out.println("\nShutdown signal received. Cleaning up...");
                    running = false;
                    try {
                      finalSession.close();
                      connection.close();
                    } catch (Exception e) {
                      e.printStackTrace();
                    }
                    System.out.println("Graceful shutdown complete.");
                  }));

      // 3. Main polling loop
      while (running) {
        Message message = consumer.receive(2000); // wait up to 2s for new messages

        if (message != null) {
          if (message instanceof TextMessage textMessage) {
            System.out.println("Received: " + textMessage.getText());
          } else {
            System.out.println("Received non-text message: " + message);
          }
        }
      }

      System.out.println("Listener stopped.");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void onMessage(Message message) {
    try {
      if (message instanceof TextMessage textMessage) {
        System.out.println("Received message: " + textMessage.getText());
      } else {
        System.out.println("Received non-text message: " + message);
      }
    } catch (JMSException e) {
      e.printStackTrace();
    }
  }
}
