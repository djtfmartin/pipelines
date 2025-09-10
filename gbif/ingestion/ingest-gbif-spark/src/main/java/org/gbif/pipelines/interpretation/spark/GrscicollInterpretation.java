package org.gbif.pipelines.interpretation.spark;

import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareValue;

import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.kvs.grscicoll.GrscicollLookupRequest;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.interpretation.transform.GrscicollTransform;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import scala.Tuple2;

@Slf4j
public class GrscicollInterpretation {

  /** Transforms the source records into the location records using the geocode service. */
  public static Dataset<GrscicollRecord> grscicollTransform(
      PipelinesConfig config,
      SparkSession spark,
      Dataset<ExtendedRecord> source,
      MetadataRecord mdr,
      int numPartitions) {

    GrscicollTransform transform = GrscicollTransform.builder().config(config).build();

    // extract the
    log.info("Extracting Grscicoll lookups from the source records");
    spark
        .sparkContext()
        .setJobGroup("grscicoll", "Extracting Grscicoll lookups from the source records", true);
    Dataset<RecordWithGrscicollLookup> recordWithLookup =
        source.map(
            (MapFunction<ExtendedRecord, RecordWithGrscicollLookup>)
                er -> {
                  GrscicollLookupRequest lookup = buildFrom(er, mdr);
                  return RecordWithGrscicollLookup.builder()
                      .id(er.getId())
                      .hash(hash(lookup))
                      .grscicollLookupRequest(lookup)
                      .build();
                },
            Encoders.bean(RecordWithGrscicollLookup.class));

    // get the distinct lookups
    log.info("Getting distinct Grscicoll lookups");
    spark.sparkContext().setJobGroup("grscicoll", "Getting distinct Grscicoll lookups", true);
    Dataset<RecordWithGrscicollLookup> distinctLookups =
        recordWithLookup.dropDuplicates("hash").repartition(numPartitions);

    // look up in kv store
    log.info("Looking up Grscicoll records");
    spark.sparkContext().setJobGroup("grscicoll", "Looking up Grscicoll records", true);
    Dataset<KeyedGrscicollRecord> keyed =
        distinctLookups.map(
            (MapFunction<RecordWithGrscicollLookup, KeyedGrscicollRecord>)
                request -> {

                  // HACK - the function takes ExtendedRecord, but we have a Location
                  ExtendedRecord er =
                      ExtendedRecord.newBuilder()
                          .setId("UNUSED_BUT_NECESSARY")
                          .setCoreTerms(coreTermsMap(request.getGrscicollLookupRequest()))
                          .build();

                  // look them up
                  Optional<GrscicollRecord> converted = transform.convert(er, mdr);
                  if (converted.isPresent()) {
                    return KeyedGrscicollRecord.builder()
                        .key(request.getHash())
                        .grscicollRecord(converted.get())
                        .build();
                  } else {
                    return KeyedGrscicollRecord.builder()
                        .key(request.getHash())
                        .build(); // TODO: null handling?
                  }
                },
            Encoders.bean(KeyedGrscicollRecord.class));

    // join the dictionary back to the source records
    spark
        .sparkContext()
        .setJobGroup("grscicoll", "Join the dictionary back to the source records", true);
    return recordWithLookup
        .joinWith(keyed, recordWithLookup.col("hash").equalTo(keyed.col("key")), "left_outer")
        .map(
            (MapFunction<Tuple2<RecordWithGrscicollLookup, KeyedGrscicollRecord>, GrscicollRecord>)
                t -> {
                  RecordWithGrscicollLookup rwl = t._1();
                  KeyedGrscicollRecord klr = t._2();

                  GrscicollRecord record =
                      (klr != null && klr.getGrscicollRecord() != null)
                          ? klr.getGrscicollRecord()
                          : GrscicollRecord.newBuilder().build();

                  record.setId(rwl.getId());
                  return record;
                },
            Encoders.bean(GrscicollRecord.class));
  }

  public static String hash(GrscicollLookupRequest request) {
    return String.join(
        "|",
        request.getInstitutionCode(),
        request.getInstitutionId(),
        request.getOwnerInstitutionCode(),
        request.getCollectionCode(),
        request.getCollectionId(),
        request.getDatasetKey(),
        request.getCountry());
  }

  private static void putIfNotNull(Map<String, String> map, Term term, String value) {
    if (value != null) {
      map.put(term.qualifiedName(), value);
    }
  }

  public static Map<String, String> coreTermsMap(GrscicollLookupRequest r) {
    Map<String, String> map = new java.util.HashMap<>();
    putIfNotNull(map, DwcTerm.institutionID, r.getInstitutionId());
    putIfNotNull(map, DwcTerm.institutionCode, r.getInstitutionCode());
    putIfNotNull(map, DwcTerm.ownerInstitutionCode, r.getOwnerInstitutionCode());
    putIfNotNull(map, DwcTerm.collectionID, r.getCollectionId());
    putIfNotNull(map, DwcTerm.collectionCode, r.getCollectionCode());
    putIfNotNull(map, GbifTerm.datasetKey, r.getDatasetKey());
    putIfNotNull(map, DwcTerm.country, r.getCountry());
    return map;
  }

  public static GrscicollLookupRequest buildFrom(ExtendedRecord er, MetadataRecord mdr) {
    return org.gbif.kvs.grscicoll.GrscicollLookupRequest.builder()
        .withInstitutionId(extractNullAwareValue(er, DwcTerm.institutionID))
        .withInstitutionCode(extractNullAwareValue(er, DwcTerm.institutionCode))
        .withOwnerInstitutionCode(extractNullAwareValue(er, DwcTerm.ownerInstitutionCode))
        .withCollectionId(extractNullAwareValue(er, DwcTerm.collectionID))
        .withCollectionCode(extractNullAwareValue(er, DwcTerm.collectionCode))
        .withDatasetKey(mdr.getDatasetKey())
        .withCountry(mdr.getDatasetPublishingCountry())
        .build();
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class RecordWithGrscicollLookup {
    private String id;
    private String hash;
    private GrscicollLookupRequest grscicollLookupRequest;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class KeyedGrscicollRecord {
    private String key;
    private GrscicollRecord grscicollRecord;
  }
}
