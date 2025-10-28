package org.gbif.pipelines.interpretation.standalone;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.codehaus.jackson.map.ObjectMapper;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.interpretation.spark.Fragmenter;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

@Slf4j
public class FragmenterCallback implements MessageCallback<PipelinesVerbatimMessage> {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final PipelinesConfig pipelinesConfig;
  private final PipelinesHistoryClient historyClient;
  private final MessagePublisher publisher;
  private static final StepType TYPE = StepType.FRAGMENTER;

  public FragmenterCallback(PipelinesConfig pipelinesConfig, MessagePublisher publisher) {
    this.pipelinesConfig = pipelinesConfig;
    this.publisher = publisher;
    this.historyClient =
        new ClientBuilder()
            .withUrl(pipelinesConfig.getStandalone().getRegistry().getWsUrl())
            .withCredentials(
                pipelinesConfig.getStandalone().getRegistry().getUser(),
                pipelinesConfig.getStandalone().getRegistry().getPassword())
            .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
            .withExponentialBackoffRetry(Duration.ofSeconds(3L), 2d, 10)
            .withFormEncoder()
            .build(PipelinesHistoryClient.class);
  }

  @Override
  public void handleMessage(PipelinesVerbatimMessage message) {

    if (!message.getRunner().equalsIgnoreCase("STANDALONE")
        || !message.getPipelineSteps().contains(TYPE.toString())) {
      log.info(
          "Incorrect message received - runner {}, stepTypes: {}",
          message.getRunner(),
          message.getPipelineSteps());
      return;
    }

    try {
      Fragmenter.runFragmenter(
          pipelinesConfig,
          message.getDatasetUuid().toString(),
          message.getAttempt(),
          "fragmenter_standalone_" + message.getDatasetUuid(),
          "local[*]");

      // Create and send outgoing message
      PipelinesVerbatimMessage outgoingMessage = createOutgoingMessage(message);

      String nextMessageClassName = outgoingMessage.getClass().getSimpleName();
      String messagePayload = outgoingMessage.toString();
      publisher.send(new PipelinesBalancerMessage(nextMessageClassName, messagePayload));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public Class<PipelinesVerbatimMessage> getMessageClass() {
    return PipelinesVerbatimMessage.class;
  }

  public PipelinesVerbatimMessage createOutgoingMessage(PipelinesVerbatimMessage message) {

    Set<String> pipelineSteps = new HashSet<>(message.getPipelineSteps());
    pipelineSteps.remove(TYPE.name());

    return new PipelinesVerbatimMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        message.getInterpretTypes(),
        pipelineSteps,
        message.getRunner(),
        message.getEndpointType(),
        message.getExtraPath(),
        message.getValidationResult(),
        message.getResetPrefix(),
        message.getExecutionId(),
        message.getDatasetType());
  }
}
