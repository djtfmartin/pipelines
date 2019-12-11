package org.gbif.pipelines.ingest.pipelines;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.solr.SolrIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.solr.common.SolrInputDocument;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.ingest.options.BasePipelineOptions;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.ingest.utils.MetricsHandler;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.transforms.common.DefaultValuesTransform;
import org.gbif.pipelines.transforms.common.FilterExtendedRecordTransform;
import org.gbif.pipelines.transforms.common.UniqueGbifIdTransform;
import org.gbif.pipelines.transforms.common.UniqueIdTransform;
import org.gbif.pipelines.transforms.converters.OccurrenceExtensionTransform;
import org.gbif.pipelines.transforms.core.*;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MeasurementOrFactTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.specific.ALATaxonomyTransform;
import org.gbif.pipelines.transforms.specific.AustraliaSpatialTransform;
import org.slf4j.MDC;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Properties;
import java.util.Set;
import java.util.function.UnaryOperator;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Reads verbatim.avro file
 *    2) Interprets and converts avro {@link ExtendedRecord} file to:
 *      {@link MetadataRecord},
 *      {@link BasicRecord},
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
 * java -jar target/ingest-gbif-standalone-BUILD_VERSION-shaded.jar some.properties
 *
 * or pass all parameters:
 *
 * java -jar target/ingest-gbif-standalone-BUILD_VERSION-shaded.jar
 * --pipelineStep=VERBATIM_TO_ALA_INTERPRETED \
 * --properties=/some/path/to/output/ws.properties
 * --datasetId=0057a720-17c9-4658-971e-9578f3577cf5
 * --attempt=1
 * --interpretationTypes=ALL
 * --runner=SparkRunner
 * --targetPath=/some/path/to/output/
 * --inputPath=/some/path/to/output/0057a720-17c9-4658-971e-9578f3577cf5/1/verbatim.avro
 *
 * }</pre>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALASamplingPipeline {

  public static void main(String[] args) {
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    run(options);
  }

  public static void run(InterpretationPipelineOptions options ) {

    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", StepType.INTERPRETED_TO_INDEX.name());
    boolean useExtendedRecordId = options.isUseExtendedRecordId();

    String datasetId = options.getDatasetId();
    Integer attempt = options.getAttempt();
    boolean tripletValid = options.isTripletValid();
    boolean occurrenceIdValid = options.isOccurrenceIdValid();
    boolean skipRegistryCalls = options.isSkipRegisrtyCalls();
    String endPointType = options.getEndPointType();


    String targetPath = options.getTargetPath();
    String hdfsSiteConfig = options.getHdfsSiteConfig();
    Properties properties = FsUtils.readPropertiesFile(options.getHdfsSiteConfig(), options.getProperties());


    log.info("Adding step 1: Options");
    UnaryOperator<String> pathFn = t -> FsUtils.buildPathInterpretUsingTargetPath(options, t, "*" + AVRO_EXTENSION);

    Pipeline p = Pipeline.create(options);

    log.info("Adding step 2: Creating transformations");
    // Core
    MetadataTransform metadataTransform = MetadataTransform.create();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    LocationTransform locationTransform = LocationTransform.create();
    ALASamplingTransform alaSamplingTransform = ALASamplingTransform.create();

    BasicTransform basicTransform =  BasicTransform.create(properties, datasetId, tripletValid, occurrenceIdValid, useExtendedRecordId);
    UniqueGbifIdTransform gbifIdTransform = UniqueGbifIdTransform.create(useExtendedRecordId);

    Set<String> types = options.getInterpretationTypes();


    log.info("Adding step 3: Creating beam pipeline");
    PCollectionView<MetadataRecord> metadataView =
            p.apply("Read Metadata", metadataTransform.read(pathFn))
                    .apply("Convert to view", View.asSingleton());

    PCollectionView<LocationRecord> locationView =
            p.apply("Read Metadata", locationTransform.read(pathFn))
                    .apply("Convert to view", View.asSingleton());


    PCollection<ExtendedRecord> uniqueRecords = metadataTransform.metadataOnly(types) ?
            verbatimTransform.emptyCollection(p) :
            p.apply("Read ExtendedRecords", verbatimTransform.read(options.getInputPath()))
                    .apply("Read occurrences from extension", OccurrenceExtensionTransform.create())
                    .apply("Filter duplicates", UniqueIdTransform.create())
                    .apply("Set default values", DefaultValuesTransform.create(properties, datasetId, skipRegistryCalls));

    PCollectionTuple basicCollection =
            uniqueRecords.apply("Check basic transform condition", basicTransform.check(types))
                    .apply("Interpret basic", basicTransform.interpret())
                    .apply("Get invalid GBIF IDs", gbifIdTransform);

    PCollection<KV<String, ExtendedRecord>> uniqueRecordsKv =
            uniqueRecords.apply("Map verbatim to KV", verbatimTransform.toKv());

    PCollection<KV<String, BasicRecord>> uniqueBasicRecordsKv =
            basicCollection.get(gbifIdTransform.getInvalidTag())
                    .apply("Map basic to KV", basicTransform.toKv());

    SingleOutput<KV<String, CoGbkResult>, ExtendedRecord> filterByGbifIdFn =
            FilterExtendedRecordTransform.create(verbatimTransform.getTag(), basicTransform.getTag()).filter();


    PCollection<ExtendedRecord> filteredUniqueRecords =
            KeyedPCollectionTuple
                    // Core
                    .of(verbatimTransform.getTag(), uniqueRecordsKv)
                    .and(basicTransform.getTag(), uniqueBasicRecordsKv)
                    // Apply
                    .apply("Grouping objects", CoGroupByKey.create())
                    .apply("Filter verbatim", filterByGbifIdFn);


    PCollection<KV<String, LocationRecord>> locationCollection =
            p.apply("Read Location", locationTransform.read(pathFn))
                    .apply("Map Location to KV", locationTransform.toKv());


    //TODO this is complaining about return types....
//    filteredUniqueRecords
//            .apply("Check ALA taxonomy transform condition", alaSamplingTransform.check(types))
//            .apply("Interpret ALA taxonomy", alaSamplingTransform.interpret())
//            .apply("Write ALA taxon to avro", alaSamplingTransform.write(pathFn));

  }
}
