package org.gbif.pipelines.interpretation.standalone;

import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.interpretation.spark.Indexing;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;

@Slf4j
public class EventsIndexingDistributedCallback extends EventsIndexingCallback {

  public EventsIndexingDistributedCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher) {
    super(pipelinesConfig, publisher);
  }

  @Override
  protected void runPipeline(PipelinesEventsInterpretedMessage message) throws Exception {
    if (pipelinesConfig.getStandalone().getIndexName() == null) {
      throw new RuntimeException("Index Name is null");
    }
    Indexing.runIndexing(
        sparkSession,
        fileSystem,
        pipelinesConfig,
        message.getDatasetUuid().toString(),
        message.getAttempt(),
        pipelinesConfig.getStandalone().getIndexName(),
        "elasticsearch/es-occurrence-schema.json",
        pipelinesConfig.getStandalone().getNumberOfShards(),
        OccurrenceJsonRecord.class,
        "json");
  }

  @Override
  protected boolean isStandalone() {
    return false;
  }
}
