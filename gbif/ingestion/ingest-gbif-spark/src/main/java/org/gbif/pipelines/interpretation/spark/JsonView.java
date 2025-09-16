package org.gbif.pipelines.interpretation.spark;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.gbif.pipelines.core.converters.OccurrenceJsonConverter;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class JsonView {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  public static Dataset<OccurrenceJsonRecord> transformToJsonView(
      Dataset<Row> records, MetadataRecord metadataRecord) {
    return records.map(
        (MapFunction<Row, OccurrenceJsonRecord>)
            record -> {
              OccurrenceJsonConverter c =
                  OccurrenceJsonConverter.builder()
                      .metadata(metadataRecord)
                      .verbatim(
                          MAPPER.readValue((String) record.getAs("verbatim"), ExtendedRecord.class))
                      .basic(MAPPER.readValue((String) record.getAs("basic"), BasicRecord.class))
                      .location(
                          MAPPER.readValue((String) record.getAs("location"), LocationRecord.class))
                      .temporal(
                          MAPPER.readValue((String) record.getAs("temporal"), TemporalRecord.class))
                      .multiTaxon(
                          MAPPER.readValue(
                              (String) record.getAs("taxonomy"), MultiTaxonRecord.class))
                      .grscicoll(
                          MAPPER.readValue(
                              (String) record.getAs("grscicoll"), GrscicollRecord.class))
                      .identifier(
                          MAPPER.readValue(
                              (String) record.getAs("identifier"), IdentifierRecord.class))
                      .clustering(
                          ClusteringRecord.newBuilder()
                              .setId(record.getAs("id"))
                              .build()) // placeholder
                      .multimedia(
                          MultimediaRecord.newBuilder()
                              .setId(record.getAs("id"))
                              .build()) // placeholder
                      .build();

              return c.convert();
            },
        Encoders.bean(OccurrenceJsonRecord.class));
  }
}
