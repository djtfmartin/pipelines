package org.gbif.pipelines.interpretation.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.gbif.pipelines.core.converters.OccurrenceHdfsRecordConverter;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;

@Slf4j
public class HdfsView implements java.io.Serializable {

  public static Dataset<OccurrenceHdfsRecord> transformToHdfsView(
      Dataset<OccurrenceRecord> records, MetadataRecord metadataRecord) {

    return records.map(
        (MapFunction<OccurrenceRecord, OccurrenceHdfsRecord>)
            row -> {
              BasicRecord basic = row.getBasic();
              LocationRecord location = row.getLocation();
              MultiTaxonRecord multi = row.getMultiTaxon();
              TemporalRecord temporal = row.getTemporal();
              GrscicollRecord grscicollRecord = row.getGrscicoll();

              log.debug("Converting id={}", basic.getId());
              OccurrenceHdfsRecordConverter c =
                  OccurrenceHdfsRecordConverter.builder()
                      .metadataRecord(metadataRecord)
                      .basicRecord(basic)
                      .locationRecord(location)
                      .temporalRecord(temporal)
                      .multiTaxonRecord(multi)
                      .grscicollRecord(grscicollRecord)
                      .identifierRecord(row.getIdentifier())
                      .build();
              return c.convert();
            },
        Encoders.bean(OccurrenceHdfsRecord.class));
  }
}
