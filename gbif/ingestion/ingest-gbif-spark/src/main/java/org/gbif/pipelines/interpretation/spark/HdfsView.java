package org.gbif.pipelines.interpretation.spark;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
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

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  public static Dataset<OccurrenceHdfsRecord> transformJsonToHdfsView(
      Dataset<Row> records, MetadataRecord metadataRecord) {
    MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    return records.map(
        (MapFunction<Row, OccurrenceHdfsRecord>)
            record -> {
              return OccurrenceHdfsRecordConverter.builder()
                  .metadataRecord(metadataRecord)
                  .extendedRecord(
                      MAPPER.readValue((String) record.getAs("verbatim"), ExtendedRecord.class))
                  .basicRecord(MAPPER.readValue((String) record.getAs("basic"), BasicRecord.class))
                  .locationRecord(
                      MAPPER.readValue((String) record.getAs("location"), LocationRecord.class))
                  .temporalRecord(
                      MAPPER.readValue((String) record.getAs("temporal"), TemporalRecord.class))
                  .multiTaxonRecord(
                      MAPPER.readValue((String) record.getAs("taxonomy"), MultiTaxonRecord.class))
                  .grscicollRecord(
                      MAPPER.readValue((String) record.getAs("grscicoll"), GrscicollRecord.class))
                  .identifierRecord(
                      MAPPER.readValue((String) record.getAs("identifier"), IdentifierRecord.class))
                  .build()
                  .convert();
            },
        Encoders.bean(OccurrenceHdfsRecord.class));
  }
}
