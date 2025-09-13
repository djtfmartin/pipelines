package org.gbif.pipelines.interpretation.spark;

import java.io.Serializable;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
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
      Dataset<OccurrenceRecordKyro> records, MetadataRecord metadataRecord) {
    return records.map(
        (MapFunction<OccurrenceRecordKyro, OccurrenceHdfsRecord>)
            record ->
                OccurrenceHdfsRecordConverter.builder()
                    .metadataRecord(metadataRecord)
                    .extendedRecord(record.getVerbatim())
                    .basicRecord(KryoUtils.deserialize(record.getBasic(), BasicRecord.class))
                    .locationRecord(
                        KryoUtils.deserialize(record.getLocation(), LocationRecord.class))
                    .temporalRecord(
                        KryoUtils.deserialize(record.getTemporal(), TemporalRecord.class))
                    .multiTaxonRecord(
                        KryoUtils.deserialize(record.getMultiTaxon(), MultiTaxonRecord.class))
                    .grscicollRecord(
                        KryoUtils.deserialize(record.getGrscicoll(), GrscicollRecord.class))
                    .identifierRecord(
                        KryoUtils.deserialize(record.getIdentifier(), IdentifierRecord.class))
                    .build()
                    .convert(),
        Encoders.bean(OccurrenceHdfsRecord.class));
  }
}
