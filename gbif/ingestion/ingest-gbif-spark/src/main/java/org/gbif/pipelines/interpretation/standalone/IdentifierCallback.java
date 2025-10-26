package org.gbif.pipelines.interpretation.standalone;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import org.codehaus.jackson.map.ObjectMapper;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.interpretation.spark.ValidateIdentifiers;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

public class IdentifierCallback implements MessageCallback<PipelinesVerbatimMessage> {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final PipelinesConfig pipelinesConfig;
  private final PipelinesHistoryClient historyClient;
  private final MessagePublisher publisher;
  private static final StepType TYPE = StepType.VERBATIM_TO_IDENTIFIER;

  public IdentifierCallback(PipelinesConfig pipelinesConfig, MessagePublisher publisher) {
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

    try {
      ValidateIdentifiers.runValidation(
          pipelinesConfig,
          message.getDatasetUuid().toString(),
          message.getAttempt(),
          "identifiers_standalone_" + message.getDatasetUuid(),
          "local[*]",
          1,
          message.getValidationResult().isTripletValid(),
          message.getValidationResult().isOccurrenceIdValid(),
          message.getValidationResult().isUseExtendedRecordId() != null
              ? message.getValidationResult().isUseExtendedRecordId()
              : false);

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
