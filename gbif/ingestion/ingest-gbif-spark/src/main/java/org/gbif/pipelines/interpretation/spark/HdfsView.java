package org.gbif.pipelines.interpretation.spark;

import java.io.Serializable;

import com.fasterxml.jackson.databind.ObjectMapper;
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

  final static ObjectMapper objectMapper = new ObjectMapper();

    public static Dataset<OccurrenceHdfsRecord> transformJsonToHdfsView(
            Dataset<OccurrenceRecordJSON> records, MetadataRecord metadataRecord) {
        return records.map(
                (MapFunction<OccurrenceRecordJSON, OccurrenceHdfsRecord>)
                        record ->
                                OccurrenceHdfsRecordConverter.builder()
                                        .metadataRecord(metadataRecord)
                                        .basicRecord(objectMapper.readValue(record.getBasic(), BasicRecord.class))
                                        .locationRecord(objectMapper.readValue(record.getLocation(), LocationRecord.class))
                                        .temporalRecord(objectMapper.readValue(record.getTemporal(), TemporalRecord.class))
                                        .multiTaxonRecord(objectMapper.readValue(record.getMultiTaxon(), MultiTaxonRecord.class))
                                        .grscicollRecord(objectMapper.readValue(record.getGrscicoll(), GrscicollRecord.class))
                                        .identifierRecord(objectMapper.readValue(record.getIdentifier(), IdentifierRecord.class))
                                        .build()
                                        .convert(),
                Encoders.bean(OccurrenceHdfsRecord.class));
    }
}
