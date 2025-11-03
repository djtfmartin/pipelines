package org.gbif.pipelines.tasks.common.hdfs;

import com.google.common.base.Strings;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.InterpretationType.RecordType;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretationMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.airflow.AppName;
import org.gbif.pipelines.common.process.*;
import org.gbif.pipelines.tasks.events.interpretation.EventsInterpretationConfiguration;
import org.gbif.pipelines.tasks.occurrences.interpretation.InterpreterConfiguration;
import org.gbif.pipelines.tasks.verbatims.dwca.DwcaToAvroConfiguration;

/** Callback which is called when an instance {@link PipelinesInterpretationMessage} is received. */
@Slf4j
@AllArgsConstructor(staticName = "create")
public class CommonHdfsViewCallback {

  private final HdfsViewConfiguration config;
  private final ExecutorService executor;

  /** Main message processing logic, creates a terminal java process, which runs */
  public Runnable createRunnable(PipelinesInterpretationMessage message) {
    return () -> {
      try {

        // If there is one step only like metadata, we have to run pipelines steps
        message.setInterpretTypes(swapInterpretTypes(message.getInterpretTypes()));

        Predicate<StepRunner> runnerPr = sr -> config.processRunner.equalsIgnoreCase(sr.name());

        log.info("Start the process. Message - {}", message);
        if (runnerPr.test(StepRunner.DISTRIBUTED)) {
          runDistributed(message);
        } else {
          throw new PipelinesException("This call back only expects DISTRIBUTED");
        }
      } catch (Exception ex) {
        log.error(ex.getMessage(), ex);
        throw new IllegalStateException(
            "Failed interpretation on " + message.getDatasetUuid().toString(), ex);
      }
    };
  }

  /**
   * Only correct messages can be handled, by now is only messages with the same runner as runner in
   * service config {@link HdfsViewConfiguration#processRunner}
   */
  public boolean isMessageCorrect(PipelinesInterpretationMessage message, StepType type) {
    if (Strings.isNullOrEmpty(message.getRunner())) {
      throw new IllegalArgumentException("Runner can't be null or empty " + message);
    }

    if (!config.processRunner.equals(message.getRunner())) {
      log.warn("Skipping, because runner is incorrect");
      return false;
    }

    if (!message.getPipelineSteps().contains(type.name())) {
      log.warn("The message doesn't contain {} type", type);
      return false;
    }
    return true;
  }

  private void runDistributed(PipelinesInterpretationMessage message) throws IOException {

    // Spark dynamic settings
    Long messageNumber = null;
    String metaFileName = null;
    if (message instanceof PipelinesInterpretedMessage) {
      messageNumber = ((PipelinesInterpretedMessage) message).getNumberOfRecords();
      metaFileName = new InterpreterConfiguration().metaFileName;
    } else if (message instanceof PipelinesEventsInterpretedMessage) {
      messageNumber = ((PipelinesEventsInterpretedMessage) message).getNumberOfEventRecords();
      metaFileName = new EventsInterpretationConfiguration().metaFileName;
    }

    long interpretationRecordsNumber =
        RecordCountReader.builder()
            .stepConfig(config.stepConfig)
            .datasetKey(message.getDatasetUuid().toString())
            .attempt(message.getAttempt().toString())
            .messageNumber(messageNumber)
            .metaFileName(metaFileName)
            .metricName(Metrics.BASIC_RECORDS_COUNT + Metrics.ATTEMPTED)
            .alternativeMetricName(Metrics.UNIQUE_GBIF_IDS_COUNT + Metrics.ATTEMPTED)
            .skipIf(true)
            .build()
            .get();

    long dwcaRecordsNumber =
        RecordCountReader.builder()
            .stepConfig(config.stepConfig)
            .datasetKey(message.getDatasetUuid().toString())
            .attempt(message.getAttempt().toString())
            .metaFileName(new DwcaToAvroConfiguration().metaFileName)
            .metricName(Metrics.ARCHIVE_TO_OCC_COUNT)
            .alternativeMetricName(Metrics.ARCHIVE_TO_ER_COUNT)
            .skipIf(true)
            .build()
            .get();

    if (interpretationRecordsNumber == 0 && dwcaRecordsNumber == 0) {
      throw new PipelinesException(
          "No data to index. Both interpretationRecordsNumber and dwcaRecordsNumber have 0 records, check metadata yaml files");
    }

    long recordsNumber = Math.min(dwcaRecordsNumber, interpretationRecordsNumber);
    if (interpretationRecordsNumber == 0) {
      recordsNumber = dwcaRecordsNumber;
    } else if (dwcaRecordsNumber == 0) {
      recordsNumber = interpretationRecordsNumber;
    }

    // App name
    String sparkAppName =
        AppName.get(config.stepType, message.getDatasetUuid(), message.getAttempt());

    // create the airflow conf
    AirflowConfFactory.Conf conf =
        AirflowConfFactory.createConf(
            message.getDatasetUuid().toString(), message.getAttempt(), sparkAppName, recordsNumber);

    // Submit
    AirflowSparkLauncher.builder()
        .airflowConfiguration(config.airflowConfig)
        .sparkAppName(sparkAppName)
        .conf(conf)
        .build()
        .submitAwaitVoid();
  }

  // If there is one step only like metadata, we have to run the RecordType steps
  private Set<String> swapInterpretTypes(Set<String> interpretTypes) {
    if (interpretTypes.isEmpty()) {
      return Collections.singleton(RecordType.ALL.name());
    }
    if (interpretTypes.size() == 1 && interpretTypes.contains(RecordType.ALL.name())) {
      return Collections.singleton(RecordType.ALL.name());
    }
    if (interpretTypes.size() == 1
        && RecordType.getAllInterpretationAsString().containsAll(interpretTypes)) {
      return Collections.singleton(config.recordType.name());
    }
    return interpretTypes;
  }
}
