package org.gbif.pipelines.interpretation.spark;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.gbif.pipelines.core.converters.OccurrenceJsonConverter;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class JsonView {

  public static Dataset<OccurrenceJsonRecord> transformToJsonView(
      Dataset<OccurrenceRecord> records, MetadataRecord metadataRecord) {

    return records.map(
        (MapFunction<OccurrenceRecord, OccurrenceJsonRecord>)
            row -> {
              OccurrenceJsonConverter c =
                  OccurrenceJsonConverter.builder()
                      .verbatim(row.getVerbatim())
                      .metadata(metadataRecord)
                      .basic(row.getBasic())
                      .location(row.getLocation())
                      .temporal(row.getTemporal())
                      .multiTaxon(row.getMultiTaxon())
                      .grscicoll(row.getGrscicoll())
                      .clustering(
                          ClusteringRecord.newBuilder()
                              .setId(row.getVerbatim().getId())
                              .build()) // placeholder
                      .multimedia(
                          MultimediaRecord.newBuilder()
                              .setId(row.getVerbatim().getId())
                              .build()) // placeholder
                      .identifier(row.getIdentifier())
                      .build();

              return c.convert();
            },
        Encoders.bean(OccurrenceJsonRecord.class));
  }
}
