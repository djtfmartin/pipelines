package org.gbif.pipelines.interpretation.standalone;

import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

@Slf4j
public class IdentifierDistributedCallback extends IdentifierCallback {

  private static final String DAG_NAME = "gbif_pipelines_occurrence_identifiers_dag";

  public IdentifierDistributedCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher) {
    super(pipelinesConfig, publisher);
  }

  @Override
  protected void runPipeline(PipelinesVerbatimMessage message) throws Exception {
    DistributedUtil.runPipeline(
        pipelinesConfig,
        message,
        "identifiers",
        fileSystem,
        DAG_NAME,
        StepType.VERBATIM_TO_IDENTIFIER);
  }
}
