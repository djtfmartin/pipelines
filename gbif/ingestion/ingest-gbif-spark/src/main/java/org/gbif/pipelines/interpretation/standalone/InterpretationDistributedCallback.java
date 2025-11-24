package org.gbif.pipelines.interpretation.standalone;

import static org.gbif.pipelines.interpretation.standalone.PostprocessValidation.getValueByKey;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.airflow.*;
import org.gbif.pipelines.airflow.AirflowConfFactory;
import org.gbif.pipelines.airflow.AirflowSparkLauncher;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

public class InterpretationDistributedCallback extends InterpretationCallback {

  private static final String DAG_NAME = "gbif_pipelines_occurrence_interpretation_dag";

  public InterpretationDistributedCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher) {
    super(pipelinesConfig, publisher);
  }

  @Override
  protected void runPipeline(PipelinesVerbatimMessage message) throws Exception {

    String metaPath =
        String.join(
            "/",
            pipelinesConfig.getOutputPath(),
            message.getDatasetUuid().toString(),
            message.getAttempt().toString(),
            "archive-to-verbatim.yml");

    Long recordsNumber =
        Long.parseLong(
            getValueByKey(
                    fileSystem,
                    metaPath,
                    PipelinesVariables.Metrics.ARCHIVE_TO_OCC_COUNT
                        + PipelinesVariables.Metrics.ATTEMPTED)
                .orElse("0"));

    // App name
    String sparkAppName =
        AppName.get(
            StepType.VERBATIM_TO_INTERPRETED, message.getDatasetUuid(), message.getAttempt());

    // create the airflow conf
    AirflowConfFactory.Conf conf =
        AirflowConfFactory.createConf(
            pipelinesConfig,
            message.getDatasetUuid().toString(),
            message.getAttempt(),
            sparkAppName,
            recordsNumber);

    // Submit
    AirflowSparkLauncher.builder()
        .airflowConfig(pipelinesConfig.getAirflowConfig())
        .conf(conf)
        .dagName(DAG_NAME)
        .sparkAppName(sparkAppName)
        .build()
        .submitAwaitVoid();
  }
}
