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
}
