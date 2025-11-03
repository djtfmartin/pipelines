package org.gbif.pipelines.common.process;

import java.util.ArrayList;
import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public class AirflowConfFactory {

  public static Conf createConf(
      String datasetId, int attempt, String sparkAppName, long recordsNumber) {

    Conf baseConf = null;
    if (recordsNumber < 0) {
      throw new IllegalArgumentException("Number of records must be greater than zero");
    }
    if (recordsNumber < 10_000_000) {
      // Naturalis
      baseConf = LEVEL_0;
    } else if (recordsNumber < 50_000_000) {
      // Bird life Denmark
      baseConf = LEVEL_1;
    } else if (recordsNumber < 300_000_000) {
      // Inaturalist, Artportalen, Observation.org etc
      baseConf = LEVEL_2;
    } else {
      // eBird
      baseConf = LEVEL_3;
    }

    List<String> combinedArgs = new ArrayList<>();
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

  /** 100k to 10 million * */
  public static final Conf LEVEL_0 =
      new Conf.ConfBuilder()
          .args(List.of("--numberOfShards=100"))
          .driverMemoryOverheadFactor("0.10")
          .executorMemoryOverheadFactor("0.15")
          .driverCores(4)
          .executorInstances(6)
          .executorCores(10)
          .defaultParallelism(100)
          .driverMinCpu("2000m")
          .driverMaxCpu("8000m")
          .driverLimitMemory("4Gi")
          .executorMinCpu("1000m")
          .executorMaxCpu("8000m")
          .executorLimitMemory("30Gi")
          .build();

  /** 10 million records to 50 million * */
  public static final Conf LEVEL_1 =
      new Conf.ConfBuilder()
          .args(List.of("--numberOfShards=200"))
          .driverMemoryOverheadFactor("0.10")
          .executorMemoryOverheadFactor("0.15")
          .driverCores(4)
          .executorInstances(20)
          .executorCores(10)
          .defaultParallelism(200)
          .driverMinCpu("2000m")
          .driverMaxCpu("8000m")
          .driverLimitMemory("4Gi")
          .executorMinCpu("1000m")
          .executorMaxCpu("8000m")
          .executorLimitMemory("30Gi")
          .build();

  /** 50 million to 500 million - iNaturalist * */
  public static final Conf LEVEL_2 =
      new Conf.ConfBuilder()
          .args(List.of("--numberOfShards=500"))
          .driverMemoryOverheadFactor("0.10")
          .executorMemoryOverheadFactor("0.15")
          .driverCores(4)
          .executorInstances(30)
          .executorCores(10)
          .defaultParallelism(500)
          .driverMinCpu("2000m")
          .driverMaxCpu("8000m")
          .driverLimitMemory("4Gi")
          .executorMinCpu("1000m")
          .executorMaxCpu("8000m")
          .executorLimitMemory("30Gi")
          .build();

  public static final Conf LEVEL_3 =
      new Conf.ConfBuilder()
          .args(List.of("--numberOfShards=1000"))
          .driverMemoryOverheadFactor("0.10")
          .executorMemoryOverheadFactor("0.15")
          .driverCores(4)
          .executorInstances(40)
          .executorCores(10)
          .defaultParallelism(1000)
          .driverMinCpu("2000m")
          .driverMaxCpu("8000m")
          .driverLimitMemory("4Gi")
          .executorMinCpu("1000m")
          .executorMaxCpu("8000m")
          .executorLimitMemory("30Gi")
          .build();

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
