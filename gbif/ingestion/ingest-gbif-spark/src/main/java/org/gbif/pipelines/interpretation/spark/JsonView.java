package org.gbif.pipelines.interpretation.spark;

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

  public static Dataset<OccurrenceJsonRecord> transformToJsonView(
      Dataset<OccurrenceRecordKyro> records, MetadataRecord metadataRecord) {
    return records.map(
        (MapFunction<OccurrenceRecordKyro, OccurrenceJsonRecord>)
            record -> {
              OccurrenceJsonConverter c =
                  OccurrenceJsonConverter.builder()
                      .metadata(metadataRecord)
                      .verbatim(record.getVerbatim())
                      .basic(KryoUtils.deserialize(record.getBasic(), BasicRecord.class))
                      .location(KryoUtils.deserialize(record.getLocation(), LocationRecord.class))
                      .temporal(KryoUtils.deserialize(record.getTemporal(), TemporalRecord.class))
                      .multiTaxon(
                          KryoUtils.deserialize(record.getMultiTaxon(), MultiTaxonRecord.class))
                      .grscicoll(
                          KryoUtils.deserialize(record.getGrscicoll(), GrscicollRecord.class))
                      .identifier(
                          KryoUtils.deserialize(record.getIdentifier(), IdentifierRecord.class))
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
