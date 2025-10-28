package org.gbif.pipelines.interpretation.standalone;

import com.fasterxml.jackson.core.JsonParseException;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.codehaus.jackson.map.ObjectMapper;
import org.gbif.api.model.pipelines.*;
import org.gbif.api.model.pipelines.ws.PipelineProcessParameters;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;
import org.slf4j.MDC;

@Slf4j
public abstract class PipelinesCallback<
        I extends PipelineBasedMessage, O extends PipelineBasedMessage>
    implements MessageCallback<I> {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  protected final PipelinesConfig pipelinesConfig;
  protected final PipelinesHistoryClient historyClient;
  protected final MessagePublisher publisher;

  private static final Set<PipelineStep.Status> FINISHED_STATE_SET =
      new HashSet<>(
          Arrays.asList(
              PipelineStep.Status.COMPLETED,
              PipelineStep.Status.ABORTED,
              PipelineStep.Status.FAILED));

  private static final Set<PipelineStep.Status> PROCESSED_STATE_SET =
      new HashSet<>(
          Arrays.asList(
              PipelineStep.Status.RUNNING,
              PipelineStep.Status.FAILED,
              PipelineStep.Status.COMPLETED,
              PipelineStep.Status.ABORTED));

  private static final Retry RETRY =
      Retry.of(
          "registryCall",
          RetryConfig.custom()
              .maxAttempts(15)
              .retryExceptions(JsonParseException.class, IOException.class, TimeoutException.class)
              .intervalFunction(
                  IntervalFunction.ofExponentialBackoff(
                      Duration.ofSeconds(1), 2d, Duration.ofSeconds(30)))
              .build());

  private static final Retry RUNNING_EXECUTION_CALL =
      Retry.of(
          "runningExecutionCall",
          RetryConfig.custom()
              .maxAttempts(15)
              .retryExceptions(JsonParseException.class, IOException.class, TimeoutException.class)
              .intervalFunction(
                  IntervalFunction.ofExponentialBackoff(
                      Duration.ofSeconds(1), 2d, Duration.ofSeconds(30)))
              .retryOnResult(Objects::isNull)
              .build());

  public PipelinesCallback(PipelinesConfig pipelinesConfig, MessagePublisher publisher) {
    this.pipelinesConfig = pipelinesConfig;
    this.publisher = publisher;
    this.historyClient =
        new ClientBuilder()
            .withUrl(pipelinesConfig.getStandalone().getRegistry().getWsUrl())
            .withCredentials(
                pipelinesConfig.getStandalone().getRegistry().getUser(),
                pipelinesConfig.getStandalone().getRegistry().getPassword())
            .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
            .withExponentialBackoffRetry(Duration.ofSeconds(3L), 2d, 10)
            .withFormEncoder()
            .build(PipelinesHistoryClient.class);
  }

  protected abstract StepType getStepType();

  protected boolean isMessageCorrect(I message) {
    if (!message.getPipelineSteps().contains(getStepType().name())) {
      log.error("The message doesn't contain {} type", getStepType().name());
      return false;
    }
    return true;
  }

  protected abstract void runPipeline(I message) throws Exception;

  public void handleMessage(I message) {

    if (!isMessageCorrect(message) || isProcessingStopped(message)) {
      log.info(
          "Skip the message, please check that message is correct/runner/validation info/etc, exit from handler");
      return;
    }

    TrackingInfo trackingInfo = null;

    try (MDC.MDCCloseable mdc =
            MDC.putCloseable("datasetKey", message.getDatasetUuid().toString());
        MDC.MDCCloseable mdc1 = MDC.putCloseable("attempt", message.getAttempt().toString());
        MDC.MDCCloseable mdc2 = MDC.putCloseable("step", getStepType().name())) {
      log.info("Processing attempt {}", message.getAttempt());

      trackingInfo = trackPipelineStep(message);

      // Run pipeline for this callback
      runPipeline(message);

      // Acknowledge message processing
      updateTrackingStatus(trackingInfo, message, PipelineStep.Status.COMPLETED);

      // Create and send outgoing message
      O outgoingMessage = createOutgoingMessage(message);

      String nextMessageClassName = outgoingMessage.getClass().getSimpleName();
      String messagePayload = outgoingMessage.toString();

      publisher.send(new PipelinesBalancerMessage(nextMessageClassName, messagePayload));

      String logInfo =
          "Next message has been sent - "
              + outgoingMessage.getClass().getSimpleName()
              + ":"
              + outgoingMessage;
      log.debug(logInfo);

      updateQueuedStatus(trackingInfo, message);

      log.info("Finished processing datasetKey: {}", message.getDatasetUuid());

    } catch (Exception ex) {

      try {
        // FIXMETrackingInfo trackingInfo = trackPipelineStep(message);
        String error =
            "Error for datasetKey - " + message.getDatasetUuid() + " : " + ex.getMessage();
        log.error(error, ex);

        // update tracking status
        if (trackingInfo != null) {
          updateTrackingStatus(trackingInfo, message, PipelineStep.Status.FAILED);
        }

      } catch (Exception e) {
        log.error(
            "Failed to update tracking status for datasetKey - " + message.getDatasetUuid(), e);
      }
      //
      //                // update validator info
      //                String errorMessage = null;
      //                if (ex.getCause() instanceof PipelinesException) {
      //                    errorMessage = ((PipelinesException) ex.getCause()).getShortMessage();
      //                }
      //                updateValidatorInfoStatus(Status.FAILED, errorMessage);
    } finally {
      if (message.getExecutionId() != null) {
        log.info("Mark execution as FINISHED if all steps are FINISHED");
        Runnable r =
            () -> {
              log.debug(
                  "History client: mark pipeline execution if finished, executionId {}",
                  message.getExecutionId());
              historyClient.markPipelineExecutionIfFinished(message.getExecutionId());
            };
        Retry.decorateRunnable(RETRY, r).run();
      }
    }
  }

  private boolean isProcessingStopped(I message) {

    Long currentKey = message.getExecutionId();

    Supplier<Long> s = () -> historyClient.getRunningExecutionKey(message.getDatasetUuid());
    Long runningKey;
    if (currentKey == null) {
      runningKey = s.get();
    } else {
      // if current key is not null, running key must not be null unless execution was aborted,
      // check multiple times
      runningKey = RUNNING_EXECUTION_CALL.executeSupplier(s);
    }
    if (currentKey == null && runningKey == null) {
      log.info("Continue execution. New execution and no other running executions");
      return false;
    }
    if (currentKey == null) {
      log.warn("Can't run new execution if some other execution is running");
      return true;
    }
    if (runningKey == null) {
      log.warn("Stop execution. Execution is aborted");
      return true;
    }
    // Stop the process if execution keys are different
    return !currentKey.equals(runningKey);
  }

  private void updateTrackingStatus(
      TrackingInfo trackingInfo, I message, PipelineStep.Status status) {

    //        String path =
    //                HdfsUtils.buildOutputPathAsString(
    //                        config.getRepositoryPath(), ti.datasetId, ti.attempt,
    // config.getMetaFileName());

    //        HdfsConfigs hdfsConfigs =
    //                HdfsConfigs.create(config.getHdfsSiteConfig(), config.getCoreSiteConfig());

    //        List<PipelineStep.MetricInfo> metricInfos =
    // HdfsUtils.readMetricsFromMetaFile(hdfsConfigs, path);

    Function<Long, PipelineStep> getPipelineStepFn =
        sk -> {
          log.debug("History client: get steps by execution key {}", sk);
          return historyClient.getPipelineStep(sk);
        };
    PipelineStep pipelineStep =
        Retry.decorateFunction(RETRY, getPipelineStepFn).apply(trackingInfo.stepKey);

    pipelineStep.setState(status);
    //        pipelineStep.setMetrics(new HashSet<>(metricInfos));
    //
    //        if (metricInfos.size() == 1) {
    //            Optional.ofNullable(metricInfos.get(0).getValue())
    //                    .filter(v -> !v.isEmpty())
    //                    .map(Long::parseLong)
    //                    .ifPresent(pipelineStep::setNumberRecords);
    //        } else if (metricInfos.size() > 1) {
    //            pipelineStep.setNumberRecords(-1L);
    //        }

    if (FINISHED_STATE_SET.contains(status)) {
      pipelineStep.setFinished(LocalDateTime.now());
    }

    try {
      Function<PipelineStep, Long> pipelineStepFn =
          s -> {
            log.debug("History client: update pipeline step: {}", s);
            PipelineStep step = historyClient.getPipelineStep(s.getKey());
            if (FINISHED_STATE_SET.contains(step.getState())) {
              return step.getKey();
            }
            return historyClient.updatePipelineStep(s);
          };
      long stepKey = Retry.decorateFunction(RETRY, pipelineStepFn).apply(pipelineStep);
      log.debug(
          "Step key {}, step type {} is {}",
          stepKey,
          pipelineStep.getType(),
          pipelineStep.getState());

    } catch (Exception ex) {
      // we don't want to break the crawling if the tracking fails
      log.error("Couldn't update tracking status for dataset {}", message.getDatasetUuid(), ex);
    }
  }

  public abstract O createOutgoingMessage(I message);

  private void updateQueuedStatus(TrackingInfo info, I message) {
    List<PipelinesWorkflow.Graph<StepType>.Edge> nodeEdges;
    if (false /* isValidator*/) {
      nodeEdges = PipelinesWorkflow.getValidatorWorkflow().getNodeEdges(getStepType());
    } else {
      boolean containsEvents = containsEvents(message);
      boolean containsOccurrences = message.getDatasetInfo().isContainsOccurrences();
      PipelinesWorkflow.Graph<StepType> workflow =
          PipelinesWorkflow.getWorkflow(containsOccurrences, containsEvents);
      nodeEdges = workflow.getNodeEdges(getStepType());
    }

    for (PipelinesWorkflow.Graph<StepType>.Edge e : nodeEdges) {
      PipelineStep step = info.pipelineStepMap.get(e.getNode());
      if (step != null && !PROCESSED_STATE_SET.contains(step.getState())) {
        // Call Registry to change the state to queued
        log.debug("History client: set pipeline step to QUEUED: {}", step);
        Retry.decorateRunnable(
                RETRY, () -> historyClient.setSubmittedPipelineStepToQueued(step.getKey()))
            .run();
        log.info("Step {} with step key {} as QUEUED", step.getType(), step.getKey());
      }
    }
  }

  private boolean containsEvents(I message) {
    PipelineBasedMessage.DatasetInfo datasetInfo = message.getDatasetInfo();
    boolean containsEvents = false;
    if (datasetInfo.getDatasetType() == DatasetType.SAMPLING_EVENT) {
      containsEvents = datasetInfo.isContainsEvents();
    }
    return containsEvents;
  }

  private TrackingInfo trackPipelineStep(I message) throws Exception {

    //
    //        if (isValidator) {
    //            log.info("Skipping status updating, isValidator {}", isValidator);
    //            return Optional.empty();
    //        }

    // create pipeline process. If it already exists it returns the existing one (the db query
    // does an upsert).
    UUID datasetUuid = message.getDatasetUuid();
    Integer attempt = message.getAttempt();

    Supplier<Long> pkSupplier =
        () -> {
          log.debug(
              "History client: create pipeline process, datasetKey {}, attempt {}",
              datasetUuid,
              attempt);
          return historyClient.createPipelineProcess(
              new PipelineProcessParameters(datasetUuid, attempt));
        };

    long processKey = Retry.decorateSupplier(RETRY, pkSupplier).get();

    Long executionId = message.getExecutionId();
    if (executionId == null) {
      log.info("executionId is empty, create initial pipelines execution");
      // create execution
      boolean containsEvents = containsEvents(message);
      boolean containsOccurrences = message.getDatasetInfo().isContainsOccurrences();

      log.info(
          "containsOccurrences: {}, containsEvents: {}, stepType: {}",
          containsOccurrences,
          containsEvents,
          getStepType());

      Set<StepType> stepTypes =
          PipelinesWorkflow.getWorkflow(containsOccurrences, containsEvents)
              .getAllNodesFor(Collections.singleton(getStepType()));

      PipelineExecution execution =
          new PipelineExecution().setStepsToRun(stepTypes).setCreated(LocalDateTime.now());

      Supplier<Long> executionIdSupplier =
          () -> {
            log.debug(
                "History client: add pipeline execution, processKey {}, execution {}",
                processKey,
                execution);
            return historyClient.addPipelineExecution(processKey, execution);
          };
      executionId = Retry.decorateSupplier(RETRY, executionIdSupplier).get();

      message.setExecutionId(executionId);
    }

    Function<Long, List<PipelineStep>> getStepsByExecutionKeyFn =
        ek -> {
          log.debug("History client: get steps by execution key {}", ek);
          return historyClient.getPipelineStepsByExecutionKey(ek);
        };

    List<PipelineStep> stepsByExecutionKey =
        Retry.decorateFunction(RETRY, getStepsByExecutionKeyFn).apply(executionId);

    // add step to the process
    PipelineStep step =
        stepsByExecutionKey.stream()
            .filter(ps -> ps.getType() == getStepType())
            .findAny()
            .orElseThrow(
                () ->
                    new PipelinesException(
                        "History service doesn't contain stepType: " + getStepType()));

    if (PROCESSED_STATE_SET.contains(step.getState())) {
      log.error(
          "Dataset is in the queue, please check the pipeline-ingestion monitoring tool - {}",
          datasetUuid);
      throw new PipelinesException(
          "Dataset is in the queue, please check the pipeline-ingestion monitoring tool");
    }

    step.setMessage(OBJECT_MAPPER.writeValueAsString(message))
        .setState(PipelineStep.Status.RUNNING)
        .setRunner(StepRunner.STANDALONE)
        .setStarted(LocalDateTime.now())
        .setPipelinesVersion("SPARK_PIPELINES-1.0");

    Function<PipelineStep, Long> pipelineStepFn =
        s -> {
          log.debug("History client: update pipeline step: {}", s);
          return historyClient.updatePipelineStep(s);
        };
    long stepKey = Retry.decorateFunction(RETRY, pipelineStepFn).apply(step);

    Map<StepType, PipelineStep> pipelineStepMap =
        stepsByExecutionKey.stream()
            .collect(Collectors.toMap(PipelineStep::getType, Function.identity()));

    return TrackingInfo.builder()
        .processKey(processKey)
        .executionId(executionId)
        .pipelineStepMap(pipelineStepMap)
        .stepKey(stepKey)
        .datasetId(datasetUuid.toString())
        .attempt(attempt.toString())
        .build();
  }

  @Builder
  public static class TrackingInfo {
    long processKey;
    long executionId;
    long stepKey;
    String datasetId;
    String attempt;
    Map<StepType, PipelineStep> pipelineStepMap;
  }
}
