package org.gbif.pipelines.interpretation.standalone;

import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.interpretation.spark.TableBuild;

@Slf4j
public class TableBuildCallback extends AbstractCallback<PipelinesVerbatimMessage> {

  public TableBuildCallback(PipelinesConfig pipelinesConfig, MessagePublisher publisher) {
    super(pipelinesConfig, publisher);
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
  protected StepType getStepType() {
    return StepType.HDFS_VIEW;
  }

  @Override
  protected void runPipeline(PipelinesVerbatimMessage message) throws Exception {

    TableBuild.runTableBuild(
        pipelinesConfig,
        message.getDatasetUuid().toString(),
        message.getAttempt(),
        "hdfsview_standalone_" + message.getDatasetUuid(),
        "local[*]");
  }
}
