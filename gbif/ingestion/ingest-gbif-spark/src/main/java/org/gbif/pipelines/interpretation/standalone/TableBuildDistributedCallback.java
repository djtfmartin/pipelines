package org.gbif.pipelines.interpretation.standalone;

import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

@Slf4j
public class TableBuildDistributedCallback extends TableBuildCallback {

  private static final String DAG_NAME = "gbif_pipelines_occurrence_hdfs_view_dag";

  public TableBuildDistributedCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher) {
    super(pipelinesConfig, publisher);
  }

  @Override
  protected void runPipeline(PipelinesInterpretedMessage message) throws Exception {
    DistributedUtil.runPipeline(
        pipelinesConfig, message, "tablebuild", fileSystem, DAG_NAME, StepType.HDFS_VIEW);
  }
}
