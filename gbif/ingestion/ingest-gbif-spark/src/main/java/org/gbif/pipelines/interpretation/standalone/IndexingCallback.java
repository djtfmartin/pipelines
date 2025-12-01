package org.gbif.pipelines.interpretation.standalone;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.interpretation.spark.Indexing;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;

@Slf4j
public class IndexingCallback
    extends PipelinesCallback<PipelinesInterpretedMessage, PipelinesIndexedMessage>
    implements MessageCallback<PipelinesInterpretedMessage> {

  public IndexingCallback(PipelinesConfig pipelinesConfig, MessagePublisher publisher) {
    super(pipelinesConfig, publisher);
  }

  @Override
  protected StepType getStepType() {
    return StepType.INTERPRETED_TO_INDEX;
  }

  @Override
  protected void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {
    Indexing.configSparkSession(sparkBuilder, config);
  }

  @Override
  protected void runPipeline(PipelinesInterpretedMessage message) throws Exception {
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
        pipelinesConfig.getStandalone().getNumberOfShards(),
        OccurrenceJsonRecord.class,
        "json");
  }

  @Override
  protected String getMetaFileName() {
    return Indexing.METRICS_FILENAME;
  }

  @Override
  public Class<PipelinesInterpretedMessage> getMessageClass() {
    return PipelinesInterpretedMessage.class;
  }

  @Override
  public PipelinesIndexedMessage createOutgoingMessage(PipelinesInterpretedMessage message) {
    return new PipelinesIndexedMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        message.getPipelineSteps(),
        null, // Set in balancer cli
        null,
        message.getEndpointType());
  }
}
