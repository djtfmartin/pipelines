package org.gbif.pipelines.interpretation.standalone;

import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

@Slf4j
public class FragmenterDistributedCallback extends IndexingCallback {

  private static final String DAG_NAME = "gbif_pipelines_verbatim_fragmenter_dag";

  public FragmenterDistributedCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher) {
    super(pipelinesConfig, publisher);
  }

  @Override
  protected void runPipeline(PipelinesInterpretedMessage message) throws Exception {
    DistributedUtil.runPipeline(
        pipelinesConfig, message, "fragmenter", fileSystem, DAG_NAME, StepType.FRAGMENTER);
  }
}
