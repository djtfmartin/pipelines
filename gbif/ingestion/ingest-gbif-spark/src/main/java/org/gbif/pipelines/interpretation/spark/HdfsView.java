package org.gbif.pipelines.interpretation.spark;

import java.io.Serializable;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.gbif.pipelines.core.converters.OccurrenceHdfsRecordConverter;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class HdfsView implements Serializable {

  public static Dataset<OccurrenceHdfsRecord> transformToHdfsView(
      Dataset<OccurrenceRecord> records, MetadataRecord metadataRecord) {
    return records.map(
        (MapFunction<OccurrenceRecord, OccurrenceHdfsRecord>)
            record ->
                OccurrenceHdfsRecordConverter.builder()
                    .metadataRecord(metadataRecord)
                    .basicRecord(record.getBasic())
                    .locationRecord(record.getLocation())
                    .temporalRecord(record.getTemporal())
                    .multiTaxonRecord(record.getMultiTaxon())
                    .grscicollRecord(record.getGrscicoll())
                    .identifierRecord(record.getIdentifier())
                    .build()
                    .convert(),
        Encoders.bean(OccurrenceHdfsRecord.class));
  }

  public static Dataset<OccurrenceHdfsRecord> transformJsonToHdfsView(
      Dataset<Row> records, MetadataRecord metadataRecord) {

    return records.map(
        (MapFunction<Row, OccurrenceHdfsRecord>)
            record -> {
              return OccurrenceHdfsRecordConverter.builder()
                  .metadataRecord(metadataRecord)
                  //                  .extendedRecord(
                  //                          KryoUtils.deserialize(
                  //                              (byte[]) record.getAs("verbatim"),
                  // ExtendedRecord.class))
                  .basicRecord(
                      KryoUtils.deserialize((byte[]) record.getAs("basic"), BasicRecord.class))
                  .locationRecord(
                      KryoUtils.deserialize(
                          (byte[]) record.getAs("location"), LocationRecord.class))
                  .temporalRecord(
                      KryoUtils.deserialize(
                          (byte[]) record.getAs("temporal"), TemporalRecord.class))
                  .multiTaxonRecord(
                      KryoUtils.deserialize(
                          (byte[]) record.getAs("taxonomy"), MultiTaxonRecord.class))
                  .grscicollRecord(
                      KryoUtils.deserialize(
                          (byte[]) record.getAs("grscicoll"), GrscicollRecord.class))
                  .identifierRecord(
                      KryoUtils.deserialize(
                          (byte[]) record.getAs("identifier"), IdentifierRecord.class))
                  .build()
                  .convert();
            },
        Encoders.bean(OccurrenceHdfsRecord.class));
  }
}
