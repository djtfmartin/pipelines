package org.gbif.pipelines.interpretation.spark;

import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;
import static org.gbif.pipelines.interpretation.spark.Interpretation.getMetadataRecord;
import static org.gbif.pipelines.interpretation.spark.Interpretation.loadExtendedRecords;
import static org.gbif.pipelines.interpretation.spark.SparkUtil.getFileSystem;
import static org.gbif.pipelines.interpretation.spark.SparkUtil.getSparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.converters.MultimediaConverter;
import org.gbif.pipelines.interpretation.transform.*;
import org.gbif.pipelines.io.avro.*;
import org.slf4j.MDC;

@Slf4j
public class EventInterpretation {

  static final ObjectMapper MAPPER = new ObjectMapper();

  public static final String METRICS_FILENAME = "verbatim-to-event.yml";

  @Parameters(separators = "=")
  private static class Args {

    @Parameter(names = "--appName", description = "Application name", required = true)
    private String appName;

    @Parameter(names = "--datasetId", description = "Dataset ID", required = true)
    private String datasetId;

    @Parameter(names = "--attempt", description = "Attempt number", required = true)
    private int attempt;

    @Parameter(
        names = "--config",
        description = "Path to YAML configuration file",
        required = false)
    private String config = "/tmp/pipelines-spark.yaml";

    @Parameter(
        names = "--master",
        description = "Spark master - there for local dev only",
        required = false)
    private String master;

    @Parameter(names = "--numberOfShards", description = "Number of shards", required = false)
    private int numberOfShards = 10;

    @Parameter(
        names = {"--help", "-h"},
        help = true,
        description = "Show usage")
    private boolean help;
  }

  public static void main(String[] argsv) throws Exception {
    Args args = new Args();
    JCommander jCommander = new JCommander(args);
    jCommander.setAcceptUnknownOptions(true);
    jCommander.parse(argsv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    PipelinesConfig config = loadConfig(args.config);
    String datasetId = args.datasetId;
    int attempt = args.attempt;

    /* ############ standard init block ########## */
    SparkSession spark =
        getSparkSession(args.master, args.appName, config, Interpretation::configSparkSession);
    FileSystem fileSystem = getFileSystem(spark, config);
    /* ############ standard init block - end ########## */

    runEventInterpretation(spark, fileSystem, config, datasetId, attempt, args.numberOfShards);

    fileSystem.close();
    spark.stop();
    spark.close();
    System.exit(0);
  }

  public static void runEventInterpretation(
      SparkSession spark,
      FileSystem fileSystem,
      PipelinesConfig config,
      String datasetId,
      int attempt,
      int numberOfShards) {

    long start = System.currentTimeMillis();

    MDC.put("datasetKey", datasetId);
    log.info("Starting event interpretation");

    String inputPath = String.format("%s/%s/%d", config.getInputPath(), datasetId, attempt);
    String outputPath = String.format("%s/%s/%d", config.getOutputPath(), datasetId, attempt);

    MetadataRecord metadata = getMetadataRecord(config, datasetId);

    // Load the extended records
    Dataset<ExtendedRecord> extendedRecords =
        loadExtendedRecords(spark, config, inputPath, outputPath, numberOfShards);

    Dataset<Event> simpleRecords = null;

    runTransforms(spark, config, simpleRecords, metadata, outputPath);

    //        // write parquet for elastic
    //        toJson(interpreted, metadata).write().mode(SaveMode.Overwrite).parquet(outputPath +
    // "/json");
    //
    //        // write parquet for hdfs view
    //        toHdfs(interpreted, metadata).write().mode(SaveMode.Overwrite).parquet(outputPath +
    // "/hdfs");
    //
    //        // cleanup intermediate parquet outputs
    //        HdfsConfigs hdfsConfigs =
    //                HdfsConfigs.create(config.getHdfsSiteConfig(), config.getCoreSiteConfig());
    //        FsUtils.deleteIfExist(hdfsConfigs, outputPath + "/extended-identifiers");
    //        FsUtils.deleteIfExist(hdfsConfigs, outputPath + "/verbatim_ext_filtered");
    //
    //        // write metrics to yaml
    //        writeMetricsYaml(
    //                fs,
    //                Map.of(
    //                        PipelinesVariables.Metrics.BASIC_RECORDS_COUNT,
    //                        identifiersCount, // need to check cli coordinator to use 1
    //                        PipelinesVariables.Metrics.UNIQUE_GBIF_IDS_COUNT, identifiersCount),
    //                outputPath + "/" + METRICS_FILENAME);
    //
    //        log.info(timeAndRecPerSecond("events-interpretation", start, identifiersCount));

  }

  public static Dataset<Event> runTransforms(
      SparkSession spark,
      PipelinesConfig config,
      Dataset<Event> simpleRecords,
      MetadataRecord metadata,
      String outputPath) {

    // Used transforms
    LocationTransform locationTransform = LocationTransform.create(config);
    TemporalTransform temporalTransform = TemporalTransform.create(config);
    MultiTaxonomyTransform taxonomyTransform = MultiTaxonomyTransform.create(config);
    MultimediaTransform multimediaTransform = MultimediaTransform.create(config);
    AudubonTransform audubonTransform = AudubonTransform.create(config);
    ImageTransform imageTransform = ImageTransform.create(config);
    EventCoreTransform eventCoreTransform = EventCoreTransform.create(config);
    MeasurementOrFactTransform measurementOrFactTransform =
        MeasurementOrFactTransform.create(config);
    HumboldtTransform humboldtTransform = HumboldtTransform.create(config);

    // Loop over all records and interpret them
    Dataset<Event> interpreted =
        simpleRecords.map(
            (MapFunction<Event, Event>)
                simpleRecord -> {
                  ExtendedRecord er =
                      MAPPER.readValue(simpleRecord.getVerbatim(), ExtendedRecord.class);
                  IdentifierRecord idr =
                      MAPPER.readValue(simpleRecord.getIdentifier(), IdentifierRecord.class);

                  // Apply all transforms
                  //                                    ExtendedRecord verbatim =
                  // defaultValuesTransform.convert(er);
                  MultiTaxonRecord tr = taxonomyTransform.convert(er);
                  LocationRecord lr = locationTransform.convert(er, metadata);
                  TemporalRecord ter = temporalTransform.convert(er);
                  MultimediaRecord mr = multimediaTransform.convert(er);
                  ImageRecord ir = imageTransform.convert(er);
                  AudubonRecord ar = audubonTransform.convert(er);
                  MeasurementOrFactRecord mfr = measurementOrFactTransform.convert(er);
                  EventCoreRecord ecr = eventCoreTransform.convert(er, null);
                  HumboldtRecord hr = humboldtTransform.convert(er);

                  // merge the multimedia records
                  MultimediaRecord mmr = MultimediaConverter.merge(mr, ir, ar);

                  return Event.builder()
                      //                                            .id(verbatim.getId())
                      .identifier(simpleRecord.getIdentifier())
                      //
                      // .verbatim(MAPPER.writeValueAsString(verbatim))
                      .taxon(MAPPER.writeValueAsString(tr))
                      .location(MAPPER.writeValueAsString(lr))
                      .temporal(MAPPER.writeValueAsString(ter))
                      .multimedia(MAPPER.writeValueAsString(mmr))
                      .measurementOrFact(MAPPER.writeValueAsString(mfr))
                      .eventCore(MAPPER.writeValueAsString(ecr))
                      .humboldt(MAPPER.writeValueAsString(hr))
                      .build();
                },
            Encoders.bean(Event.class));

    // write simple interpreted records to disk
    interpreted.write().mode(SaveMode.Overwrite).parquet(outputPath + "/simple-event");

    // re-load
    return spark.read().parquet(outputPath + "/simple-event").as(Encoders.bean(Event.class));
  }
}
