package org.gbif.pipelines.interpretation.spark;

import static org.gbif.dwc.terms.DwcTerm.parentEventID;
import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareValue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractValue;

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
import org.gbif.pipelines.io.avro.IssueRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import scala.Tuple2;

@Slf4j
public class GrscicollInterpretation {

  /** Transforms the source records into the location records using the geocode service. */
  public static Dataset<OccurrenceRecord> grscicollTransform(
      PipelinesConfig config,
      SparkSession spark,
      Dataset<OccurrenceRecord> source,
      MetadataRecord mdr) {

    GrscicollTransform transform =
        GrscicollTransform.builder()
            .gbifApiUrl(config.getGrscicollLookup().getApi().getWsUrl())
            .build();

    // extract the
    log.info("Extracting Grscicoll lookups from the source records");
    Dataset<RecordWithGrscicollLookup> recordWithLookup =
        source.map(
            (MapFunction<OccurrenceRecord, RecordWithGrscicollLookup>)
                er -> {
                  GrscicollLookupRequest lookup = buildFrom(er.getVerbatim(), mdr);
                  return RecordWithGrscicollLookup.builder()
                      .id(er.getVerbatim().getId())
                      .coreId(er.getVerbatim().getCoreId())
                      .parentId(extractValue(er.getVerbatim(), parentEventID))
                      .hash(hash(lookup))
                      .grscicollLookupRequest(lookup)
                      .build();
                },
            Encoders.bean(RecordWithGrscicollLookup.class));
    recordWithLookup.createOrReplaceTempView("record_with_grscicollLookup");

    // get the distinct lookups
    log.info("Getting distinct Grscicoll lookups");
    Dataset<RecordWithGrscicollLookup> distinctLookups =
        recordWithLookup
            .dropDuplicates("hash")
            .repartition(config.getGrscicollLookup().getParallelism());

    // look up in kv store
    log.info("Looking up Grscicoll records");
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
    keyed.createOrReplaceTempView("key_grscicollrecord");

    // join the dictionary back to the source records
    log.info("Joining back Grscicoll records to the source records");
    Dataset<RecordWithGrscicollRecord> expanded =
        spark
            .sql(
                "SELECT r.id, r.coreId, r.parentId, l.grscicollRecord"
                    + " FROM record_with_grscicollLookup r "
                    + " LEFT JOIN key_grscicollrecord l ON r.hash = l.key")
            .as(Encoders.bean(RecordWithGrscicollRecord.class));

    log.info("Merging Grscicoll records to the occurrence records");
    Dataset<GrscicollRecord> grscicollRecords =
        expanded.map(
            (MapFunction<RecordWithGrscicollRecord, GrscicollRecord>)
                r -> {
                  GrscicollRecord grscicollRecord =
                      r.getGrscicollRecord() == null
                          ? GrscicollRecord.newBuilder().build()
                          : r.getGrscicollRecord();

                  grscicollRecord.setId(r.getId());

                  if (grscicollRecord.getIssues() == null) {
                    grscicollRecord.setIssues(IssueRecord.newBuilder().build());
                  }
                  return grscicollRecord;
                },
            Encoders.bean(GrscicollRecord.class));

    return source
        .joinWith(grscicollRecords, source.col("basic.id").equalTo(grscicollRecords.col("id")))
        .map(
            (MapFunction<Tuple2<OccurrenceRecord, GrscicollRecord>, OccurrenceRecord>)
                row -> {
                  OccurrenceRecord r = row._1;
                  r.setGrscicoll(row._2);
                  return r;
                },
            Encoders.bean(OccurrenceRecord.class));
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
    private String coreId;
    private String parentId;
    private String hash;
    private GrscicollLookupRequest grscicollLookupRequest;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class RecordWithGrscicollRecord {
    private String id;
    private String coreId;
    private String parentId;
    private GrscicollRecord grscicollRecord;
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
