package org.gbif.pipelines.interpretation.standalone;

import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

@Slf4j
public class EventsIndexingDistributedCallback extends EventsIndexingCallback {

  public EventsIndexingDistributedCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher) {
    super(pipelinesConfig, publisher);
  }

  @Override
  protected void runPipeline(PipelinesEventsInterpretedMessage message) throws Exception {
    DistributedUtil.runPipeline(
        pipelinesConfig,
        message,
        "event-indexing",
        fileSystem,
        pipelinesConfig.getAirflowConfig().eventsIndexingDag,
        StepType.EVENTS_INTERPRETED_TO_INDEX);
  }

  @Override
  protected boolean isStandalone() {
    return false;
  }
}
