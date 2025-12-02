package org.gbif.pipelines.interpretation.standalone;

import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.interpretation.spark.TableBuild;

@Slf4j
public class EventsTableBuildDistributedCallback extends EventsTableBuildCallback {

  public EventsTableBuildDistributedCallback(
      PipelinesConfig pipelinesConfig,
      MessagePublisher publisher,
      String tableName,
      String sourceDirectory) {
    super(pipelinesConfig, publisher, tableName, sourceDirectory);
  }

  @Override
  protected void runPipeline(PipelinesEventsInterpretedMessage message) throws Exception {
    TableBuild.runTableBuild(
        sparkSession,
        fileSystem,
        pipelinesConfig,
        message.getDatasetUuid().toString(),
        message.getAttempt(),
        tableName,
        sourceDirectory);
  }

  @Override
  protected boolean isStandalone() {
    return false;
  }
}
