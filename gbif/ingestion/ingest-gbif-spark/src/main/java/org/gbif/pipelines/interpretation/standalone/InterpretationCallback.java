package org.gbif.pipelines.interpretation.standalone;

import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.*;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.interpretation.spark.Interpretation;

@Slf4j
public class InterpretationCallback extends AbstractCallback<PipelinesVerbatimMessage> {

  public InterpretationCallback(PipelinesConfig pipelinesConfig, MessagePublisher publisher) {
    super(pipelinesConfig, publisher);
  }

  @Override
  protected StepType getStepType() {
    return StepType.VERBATIM_TO_INTERPRETED;
  }

  @Override
  protected boolean isMessageCorrect(PipelinesVerbatimMessage message) {
    if (!message.getRunner().equalsIgnoreCase("STANDALONE")) {
      log.info(
          "Incorrect message received - runner {}, stepTypes: {}",
          message.getRunner(),
          message.getPipelineSteps());
      return false;
    }
    return true;
  }

  @Override
  protected void runPipeline(PipelinesVerbatimMessage message) {

    log.info(
        "### Running {} for {}",
        StepType.VERBATIM_TO_INTERPRETED,
        message.getDatasetUuid().toString());

    Interpretation.runInterpretation(
        pipelinesConfig,
        message.getDatasetUuid().toString(),
        message.getAttempt(),
        1,
        "interpretation_standalone_" + message.getDatasetUuid(),
        "local[*]",
        message.getValidationResult().isTripletValid(),
        message.getValidationResult().isOccurrenceIdValid());

    log.info(
        "### Finished {} for {}",
        StepType.VERBATIM_TO_INTERPRETED,
        message.getDatasetUuid().toString());
  }
}
