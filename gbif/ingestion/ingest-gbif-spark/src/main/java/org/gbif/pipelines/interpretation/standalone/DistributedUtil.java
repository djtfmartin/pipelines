package org.gbif.pipelines.interpretation.standalone;

import static org.gbif.pipelines.interpretation.standalone.PostprocessValidation.getValueByKey;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.pipelines.airflow.AirflowConfFactory;
import org.gbif.pipelines.airflow.AirflowSparkLauncher;
import org.gbif.pipelines.airflow.AppName;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.slf4j.MDC;

@Slf4j(topic = "Pipeline")
public class DistributedUtil {

  public static void runPipeline(
      PipelinesConfig pipelinesConfig,
      PipelineBasedMessage message,
      String jobName,
      FileSystem fileSystem,
      String dagName,
      StepType stepType)
      throws Exception {

    MDC.put("datasetKey", message.getDatasetUuid().toString());
    log.info("Starting {}", jobName);

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
    String sparkAppName = AppName.get(stepType, message.getDatasetUuid(), message.getAttempt());

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
        .dagName(dagName)
        .sparkAppName(sparkAppName)
        .build()
        .submitAwaitVoid();

    MDC.put("datasetKey", message.getDatasetUuid().toString());
    log.info("Finished {}", jobName);
  }
}
