package org.gbif.pipelines.ingest.java.pipelines;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.converters.converter.SyncDataFileWriter;
import org.gbif.converters.converter.SyncDataFileWriterBuilder;
import org.gbif.pipelines.ingest.java.metrics.IngestMetrics;
import org.gbif.pipelines.ingest.java.metrics.IngestMetricsBuilder;
import org.gbif.pipelines.ingest.java.transforms.AvroReader;
import org.gbif.pipelines.ingest.java.transforms.DefaultValuesTransform;
import org.gbif.pipelines.ingest.java.transforms.OccurrenceExtensionTransform;
import org.gbif.pipelines.ingest.java.transforms.UniqueGbifIdTransform;
import org.gbif.pipelines.ingest.java.utils.PropertiesFactory;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.ingest.utils.MetricsHandler;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.transforms.SerializableConsumer;
import org.gbif.pipelines.transforms.Transform;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.MetadataTransform;
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MeasurementOrFactTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.MDC;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.converters.converter.FsUtils.createParentDirectories;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.ALL;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Reads verbatim.avro file
 *    2) Interprets and converts avro {@link org.gbif.pipelines.io.avro.ExtendedRecord} file to:
 *      {@link org.gbif.pipelines.io.avro.MetadataRecord},
 *      {@link org.gbif.pipelines.io.avro.BasicRecord},
 *      {@link org.gbif.pipelines.io.avro.TemporalRecord},
 *      {@link org.gbif.pipelines.io.avro.MultimediaRecord},
 *      {@link org.gbif.pipelines.io.avro.ImageRecord},
 *      {@link org.gbif.pipelines.io.avro.AudubonRecord},
 *      {@link org.gbif.pipelines.io.avro.MeasurementOrFactRecord},
 *      {@link org.gbif.pipelines.io.avro.TaxonRecord},
 *      {@link org.gbif.pipelines.io.avro.LocationRecord}
 *    3) Writes data to independent files
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -jar target/ingest-gbif-java-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.java.pipelines.VerbatimToInterpretedPipeline some.properties
 *
 * or pass all parameters:
 *
 * java -cp target/ingest-gbif-java-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.java.pipelines.VerbatimToInterpretedPipeline \
 * --datasetId=4725681f-06af-4b1e-8fff-e31e266e0a8f \
 * --attempt=1 \
 * --interpretationTypes=ALL \
 * --targetPath=/path \
 * --inputPath=/path/verbatim.avro \
 * --properties=/path/pipelines.properties \
 * --useExtendedRecordId=true
 * --skipRegisrtyCalls=true
 *
 * }</pre>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class VerbatimToInterpretedPipeline {

  public static void main(String[] args) {
    run(args);
  }

  public static void run(String[] args) {
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    run(options);
  }

  public static void run(InterpretationPipelineOptions options) {
    ExecutorService executor = Executors.newWorkStealingPool();
    try {
      run(options, executor);
    } finally {
      executor.shutdown();
    }
  }

  public static void run(String[] args, ExecutorService executor) {
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    run(options, executor);
  }

  public static void run(InterpretationPipelineOptions options, ExecutorService executor) {

    log.info("Pipeline has been started - {}", LocalDateTime.now());

    String datasetId = options.getDatasetId();
    Integer attempt = options.getAttempt();
    boolean tripletValid = options.isTripletValid();
    boolean occIdValid = options.isOccurrenceIdValid();
    boolean useErdId = options.isUseExtendedRecordId();
    boolean skipRegistryCalls = options.isSkipRegisrtyCalls();
    Set<String> types = Collections.singleton(ALL.name());
    String targetPath = options.getTargetPath();
    String endPointType = options.getEndPointType();
    String hdfsSiteConfig = options.getHdfsSiteConfig();
    Properties properties = PropertiesFactory.getInstance(hdfsSiteConfig, options.getProperties()).get();

    FsUtils.deleteInterpretIfExist(hdfsSiteConfig, targetPath, datasetId, attempt, types);

    MDC.put("datasetId", datasetId);
    MDC.put("attempt", attempt.toString());
    MDC.put("step", StepType.VERBATIM_TO_INTERPRETED.name());

    String id = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));

    log.info("Init metrics");
    IngestMetrics metrics = IngestMetricsBuilder.createVerbatimToInterpretedMetrics();
    SerializableConsumer<String> incMetricFn = metrics::incMetric;

    log.info("Creating pipelines transforms");
    // Core
    MetadataTransform metadataTransform = MetadataTransform.create(properties, endPointType, attempt, skipRegistryCalls)
        .counterFn(incMetricFn).init();
    BasicTransform basicTransform = BasicTransform.create(properties, datasetId, tripletValid, occIdValid, useErdId)
        .counterFn(incMetricFn).init();
    TaxonomyTransform taxonomyTransform = TaxonomyTransform.create(properties)
        .counterFn(incMetricFn).init();
    LocationTransform locationTransform = LocationTransform.create(properties)
        .counterFn(incMetricFn).init();
    VerbatimTransform verbatimTransform = VerbatimTransform.create()
        .counterFn(incMetricFn);
    TemporalTransform temporalTransform = TemporalTransform.create()
        .counterFn(incMetricFn);
    // Extension
    MeasurementOrFactTransform measurementTransform = MeasurementOrFactTransform.create()
        .counterFn(incMetricFn);
    MultimediaTransform multimediaTransform = MultimediaTransform.create()
        .counterFn(incMetricFn);
    AudubonTransform audubonTransform = AudubonTransform.create()
        .counterFn(incMetricFn);
    ImageTransform imageTransform = ImageTransform.create()
        .counterFn(incMetricFn);
    // Extra
    OccurrenceExtensionTransform occExtensionTransform = OccurrenceExtensionTransform.create().counterFn(incMetricFn);
    DefaultValuesTransform defaultValuesTransform = DefaultValuesTransform.create(properties, datasetId, skipRegistryCalls);

    try (
        SyncDataFileWriter<ExtendedRecord> verbatimWriter =
            createWriter(options, ExtendedRecord.getClassSchema(), verbatimTransform, id, false);
        SyncDataFileWriter<MetadataRecord> metadataWriter =
            createWriter(options, MetadataRecord.getClassSchema(), metadataTransform, id, false);
        SyncDataFileWriter<BasicRecord> basicWriter =
            createWriter(options, BasicRecord.getClassSchema(), basicTransform, id, false);
        SyncDataFileWriter<BasicRecord> basicInvalidWriter =
            createWriter(options, BasicRecord.getClassSchema(), basicTransform, id, true);
        SyncDataFileWriter<TemporalRecord> temporalWriter =
            createWriter(options, TemporalRecord.getClassSchema(), temporalTransform, id, false);
        SyncDataFileWriter<MultimediaRecord> multimediaWriter =
            createWriter(options, MultimediaRecord.getClassSchema(), multimediaTransform, id, false);
        SyncDataFileWriter<ImageRecord> imageWriter =
            createWriter(options, ImageRecord.getClassSchema(), imageTransform, id, false);
        SyncDataFileWriter<AudubonRecord> audubonWriter =
            createWriter(options, AudubonRecord.getClassSchema(), audubonTransform, id, false);
        SyncDataFileWriter<MeasurementOrFactRecord> measurementWriter =
            createWriter(options, MeasurementOrFactRecord.getClassSchema(), measurementTransform, id, false);
        SyncDataFileWriter<TaxonRecord> taxonWriter =
            createWriter(options, TaxonRecord.getClassSchema(), taxonomyTransform, id, false);
        SyncDataFileWriter<LocationRecord> locationWriter =
            createWriter(options, LocationRecord.getClassSchema(), locationTransform, id, false)
    ) {

      // Create MetadataRecord
      MetadataRecord mdr = metadataTransform.processElement(options.getDatasetId())
          .orElseThrow(() -> new IllegalArgumentException("MetadataRecord can't be null"));
      metadataWriter.append(mdr);

      // Read DWCA and replace default values
      Map<String, ExtendedRecord> erMap = AvroReader.readUniqueRecords(hdfsSiteConfig, ExtendedRecord.class, options.getInputPath());
      Map<String, ExtendedRecord> erExtMap = occExtensionTransform.transform(erMap);
      defaultValuesTransform.replaceDefaultValues(erExtMap);

      boolean useSyncMode = options.getSyncThreshold() > erExtMap.size();

      // Filter GBIF id duplicates
      UniqueGbifIdTransform gbifIdTransform =
          UniqueGbifIdTransform.builder()
              .executor(executor)
              .erMap(erExtMap)
              .basicTransform(basicTransform)
              .useSyncMode(useSyncMode)
              .skipTransform(useErdId)
              .build()
              .run();

      // Create interpretation function
      Consumer<ExtendedRecord> interpretAllFn = er -> {
        BasicRecord br = gbifIdTransform.getBrInvalidMap().get(er.getId());
        if (br == null) {
          verbatimWriter.append(er);
          temporalTransform.processElement(er).ifPresent(temporalWriter::append);
          multimediaTransform.processElement(er).ifPresent(multimediaWriter::append);
          imageTransform.processElement(er).ifPresent(imageWriter::append);
          audubonTransform.processElement(er).ifPresent(audubonWriter::append);
          measurementTransform.processElement(er).ifPresent(measurementWriter::append);
          taxonomyTransform.processElement(er).ifPresent(taxonWriter::append);
          locationTransform.processElement(er, mdr).ifPresent(locationWriter::append);
        } else {
          basicInvalidWriter.append(br);
        }
      };

      // Run async writing for BasicRecords
      Stream<CompletableFuture<Void>> streamBr;
      Collection<BasicRecord> brCollection = gbifIdTransform.getBrMap().values();
      if (useSyncMode) {
        streamBr = Stream.of(CompletableFuture.runAsync(() -> brCollection.forEach(basicWriter::append), executor));
      } else {
        streamBr = brCollection.stream().map(v -> CompletableFuture.runAsync(() -> basicWriter.append(v), executor));
      }

      // Run async interpretation and writing for all records
      Stream<CompletableFuture<Void>> streamAll;
      Collection<ExtendedRecord> erCollection = erExtMap.values();
      if (useSyncMode) {
        streamAll = Stream.of(CompletableFuture.runAsync(() -> erCollection.forEach(interpretAllFn), executor));
      } else {
        streamAll = erCollection.stream().map(v -> CompletableFuture.runAsync(() -> interpretAllFn.accept(v), executor));
      }

      // Wait for all features
      CompletableFuture[] futures = Stream.concat(streamBr, streamAll).toArray(CompletableFuture[]::new);
      CompletableFuture.allOf(futures).get();

    } catch (Exception e) {
      log.error("Failed performing conversion on {}", e.getMessage());
      throw new IllegalStateException("Failed performing conversion on ", e);
    } finally {
      basicTransform.tearDown();
      taxonomyTransform.tearDown();
      locationTransform.tearDown();
    }

    MetricsHandler.saveCountersToTargetPathFile(options, metrics.getMetricsResult());
    log.info("Pipeline has been finished - {}", LocalDateTime.now());
  }

  /** Create an AVRO file writer */
  @SneakyThrows
  private static <T> SyncDataFileWriter<T> createWriter(InterpretationPipelineOptions options, Schema schema,
      Transform transform, String id, boolean useInvalidName) {
    UnaryOperator<String> pathFn = t -> FsUtils.buildPathInterpretUsingTargetPath(options, t, id + AVRO_EXTENSION);
    String baseName = useInvalidName ? transform.getBaseInvalidName() : transform.getBaseName();
    Path path = new Path(pathFn.apply(baseName));
    FileSystem fs = createParentDirectories(path, options.getHdfsSiteConfig());
    return SyncDataFileWriterBuilder.builder()
        .schema(schema)
        .codec(options.getAvroCompressionType())
        .outputStream(fs.create(path))
        .syncInterval(options.getAvroSyncInterval())
        .build()
        .createSyncDataFileWriter();
  }

}
