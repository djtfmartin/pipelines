package org.gbif.pipelines.interpretation.standalone;

import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.*;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.interpretation.spark.TableBuild;

@Slf4j
public class TableBuildCallback
    extends PipelinesCallback<PipelinesInterpretedMessage, PipelinesHdfsViewMessage>
    implements MessageCallback<PipelinesInterpretedMessage> {

  public TableBuildCallback(PipelinesConfig pipelinesConfig, MessagePublisher publisher) {
    super(pipelinesConfig, publisher);
  }

  @Override
  protected StepType getStepType() {
    return StepType.HDFS_VIEW;
  }

  @Override
  protected void runPipeline(PipelinesInterpretedMessage message) throws Exception {
    TableBuild.runTableBuild(
        pipelinesConfig,
        message.getDatasetUuid().toString(),
        message.getAttempt(),
        "hdfsview_standalone_" + message.getDatasetUuid(),
        "local[*]");
  }

  @Override
  public PipelinesHdfsViewMessage createOutgoingMessage(PipelinesInterpretedMessage message) {
    return new PipelinesHdfsViewMessage(
        message.getDatasetUuid(), message.getAttempt(), message.getPipelineSteps(), null, null);
  }

  @Override
  public Class<PipelinesInterpretedMessage> getMessageClass() {
    return PipelinesInterpretedMessage.class;
  }
}
