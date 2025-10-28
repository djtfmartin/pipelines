package org.gbif.pipelines.interpretation.standalone;

import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.*;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.interpretation.spark.Interpretation;

@Slf4j
public class InterpretationCallback
    extends PipelinesCallback<PipelinesVerbatimMessage, PipelinesInterpretedMessage>
    implements MessageCallback<PipelinesVerbatimMessage> {

  public InterpretationCallback(PipelinesConfig pipelinesConfig, MessagePublisher publisher) {
    super(pipelinesConfig, publisher);
  }

  @Override
  protected StepType getStepType() {
    return StepType.VERBATIM_TO_INTERPRETED;
  }

  @Override
  protected boolean isMessageCorrect(PipelinesVerbatimMessage message) {
    return false;
  }

  @Override
  protected void runPipeline(PipelinesVerbatimMessage message) throws Exception {

    // Run interpretation
    Interpretation.runInterpretation(
        pipelinesConfig,
        message.getDatasetUuid().toString(),
        message.getAttempt(),
        1,
        "interpretation_standalone_" + message.getDatasetUuid(),
        "local[*]",
        message.getValidationResult().isTripletValid(),
        message.getValidationResult().isOccurrenceIdValid());
  }

  public PipelinesInterpretedMessage createOutgoingMessage(PipelinesVerbatimMessage message) {

    Long recordsNumber = null;
    Long eventRecordsNumber = null;
    if (message.getValidationResult() != null) {
      recordsNumber = message.getValidationResult().getNumberOfRecords();
      eventRecordsNumber = message.getValidationResult().getNumberOfEventRecords();
    }

    // boolean repeatAttempt = pathExists(message);
    return new PipelinesInterpretedMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        message.getPipelineSteps(),
        recordsNumber,
        eventRecordsNumber,
        null, // Set in balancer cli
        false, // repeatAttempt,
        message.getResetPrefix(),
        message.getExecutionId(),
        message.getEndpointType(),
        message.getValidationResult(),
        message.getInterpretTypes(),
        message.getDatasetType());
  }

  @Override
  public Class<PipelinesVerbatimMessage> getMessageClass() {
    return PipelinesVerbatimMessage.class;
  }
}
