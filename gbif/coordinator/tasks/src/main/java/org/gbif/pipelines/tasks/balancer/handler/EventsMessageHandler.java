package org.gbif.pipelines.tasks.balancer.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesEventsMessage;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EventsMessageHandler {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static void handle(MessagePublisher publisher, PipelinesBalancerMessage message)
      throws IOException {

    log.info("Process PipelinesEventsMessage - {}", message);

    // Populate message fields
    PipelinesEventsMessage outputMessage =
        MAPPER.readValue(message.getPayload(), PipelinesEventsMessage.class);

    // FIXME - need to compute runner type
    outputMessage.setRunner(StepRunner.STANDALONE.name());

    publisher.send(outputMessage);
    log.info("The message has been sent - {}", outputMessage);
  }
}
