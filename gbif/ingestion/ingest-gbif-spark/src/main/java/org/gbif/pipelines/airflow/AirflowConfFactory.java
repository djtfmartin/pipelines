package org.gbif.pipelines.airflow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.SparkJobConfig;

@Getter
@Slf4j
public class AirflowConfFactory {

  public static Conf createConf(
      PipelinesConfig pipelinesConfig,
      String datasetId,
      int attempt,
      String sparkAppName,
      long recordsNumber) {
    return createConf(pipelinesConfig, datasetId, attempt, sparkAppName, recordsNumber, List.of());
  }

  public static Conf createConf(
      PipelinesConfig pipelinesConfig,
      String datasetId,
      int attempt,
      String sparkAppName,
      long recordsNumber,
      List<String> extraArgs) {

    Map<String, SparkJobConfig> configs = pipelinesConfig.getProcessingConfigs();

    SparkJobConfig baseConf = null;
    if (recordsNumber < 0) {
      throw new IllegalArgumentException("Number of records must be greater than zero");
    }

    Set<String> expressions = configs.keySet();
    for (String expression : expressions) {
      if (match(expression, recordsNumber)) {
        baseConf = configs.get(expression);
        break;
      }
    }

    List<String> combinedArgs = new ArrayList<>(extraArgs);
    combinedArgs.add("--datasetId=" + datasetId);
    combinedArgs.add("--attempt=" + attempt);
    combinedArgs.add("--appName=" + sparkAppName);
    combinedArgs.addAll(baseConf.getArgs());

    return Conf.builder()
        .args(combinedArgs)
        .driverMemoryOverheadFactor(baseConf.driverMemoryOverheadFactor)
        .driverCores(baseConf.driverCores)
        .executorMemoryOverheadFactor(baseConf.executorMemoryOverheadFactor)
        .executorInstances(baseConf.executorInstances)
        .executorCores(baseConf.executorCores)
        .defaultParallelism(baseConf.defaultParallelism)
        .driverMinCpu(baseConf.driverMinCpu)
        .driverMaxCpu(baseConf.driverMaxCpu)
        .driverLimitMemory(baseConf.driverLimitMemory)
        .executorMinCpu(baseConf.executorMinCpu)
        .executorMaxCpu(baseConf.executorMaxCpu)
        .executorLimitMemory(baseConf.executorLimitMemory)
        .build();
  }

  private static boolean match(String expression, Long numberOfRecords) {
    try {
      // Substitute variable
      expression = expression.replace("numberOfRecords", String.valueOf(numberOfRecords));

      ScriptEngine engine = new ScriptEngineManager().getEngineByName("JavaScript");
      return (Boolean) engine.eval(expression);
    } catch (Exception e) {
      log.error("Error evaluating expression {}", expression, e);
      throw new RuntimeException(e);
    }
  }

  @Data
  @Builder
  public static class Conf {

    // command line args
    private final List<String> args;

    // spark settings
    public final String driverMemoryOverheadFactor;
    public final int driverCores;
    public final String executorMemoryOverheadFactor;
    public final int executorInstances;
    public final int executorCores;
    public final int defaultParallelism; // should be same as number of shards

    // kubernetes settings
    public final String driverMinCpu;
    public final String driverMaxCpu;
    public final String driverLimitMemory;

    public final String executorMinCpu;
    public final String executorMaxCpu;

    public final String executorLimitMemory;
  }
}
