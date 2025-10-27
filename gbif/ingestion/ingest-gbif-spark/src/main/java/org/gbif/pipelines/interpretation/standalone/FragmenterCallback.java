package org.gbif.pipelines.interpretation.standalone;

import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.interpretation.spark.Fragmenter;

@Slf4j
public class FragmenterCallback extends AbstractCallback<PipelinesVerbatimMessage> {

  public FragmenterCallback(PipelinesConfig pipelinesConfig, MessagePublisher publisher) {
    super(pipelinesConfig, publisher);
  }

  @Override
  protected StepType getStepType() {
    return StepType.FRAGMENTER;
  }

  @Override
  protected boolean isMessageCorrect(PipelinesVerbatimMessage message) {
    if (!message.getRunner().equalsIgnoreCase("STANDALONE")
        || !message.getPipelineSteps().contains(getStepType().toString())) {
      log.info(
          "Incorrect message received - runner {}, stepTypes: {}",
          message.getRunner(),
          message.getPipelineSteps());
      return false;
    }
    return true;
  }

  @Override
  protected void runPipeline(PipelinesVerbatimMessage message) throws Exception {
    Fragmenter.runFragmenter(
        pipelinesConfig,
        message.getDatasetUuid().toString(),
        message.getAttempt(),
        "fragmenter_standalone_" + message.getDatasetUuid(),
        "local[*]");
  }
}
