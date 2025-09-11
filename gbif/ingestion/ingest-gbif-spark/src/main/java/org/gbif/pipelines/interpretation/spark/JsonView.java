package org.gbif.pipelines.interpretation.spark;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.gbif.pipelines.core.converters.OccurrenceJsonConverter;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class JsonView {

  static final ObjectMapper objectMapper = new ObjectMapper();

  public static Dataset<OccurrenceJsonRecord> transformToJsonView(
      Dataset<OccurrenceRecordJSON> records, MetadataRecord metadataRecord) {
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return records.map(
        (MapFunction<OccurrenceRecordJSON, OccurrenceJsonRecord>)
            record -> {
              OccurrenceJsonConverter c =
                  OccurrenceJsonConverter.builder()
                      .metadata(metadataRecord)
                      .verbatim(objectMapper.readValue(record.getVerbatim(), ExtendedRecord.class))
                      .basic(objectMapper.readValue(record.getBasic(), BasicRecord.class))
                      .location(objectMapper.readValue(record.getLocation(), LocationRecord.class))
                      .temporal(objectMapper.readValue(record.getTemporal(), TemporalRecord.class))
                      .multiTaxon(
                          objectMapper.readValue(record.getMultiTaxon(), MultiTaxonRecord.class))
                      .grscicoll(
                          objectMapper.readValue(record.getGrscicoll(), GrscicollRecord.class))
                      .identifier(
                          objectMapper.readValue(record.getIdentifier(), IdentifierRecord.class))
                      .clustering(
                          ClusteringRecord.newBuilder()
                              .setId(record.getId())
                              .build()) // placeholder
                      .multimedia(
                          MultimediaRecord.newBuilder()
                              .setId(record.getId())
                              .build()) // placeholder
                      .build();

              return c.convert();
            },
        Encoders.bean(OccurrenceJsonRecord.class));
  }
}
