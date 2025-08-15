package org.gbif.pipelines.interpretation.spark;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.gbif.pipelines.core.converters.OccurrenceHdfsRecordConverter;
import org.gbif.pipelines.io.avro.*;
import scala.Tuple2;

public class HdfsView implements java.io.Serializable {

  public static Dataset<OccurrenceHdfsRecord> transformToHdfsView(
      Dataset<BasicRecord> basicRecordDataset,
      Dataset<LocationRecord> locationRecordDataset,
      Dataset<MultiTaxonRecord> multiTaxonRecordDataset,
      Dataset<TemporalRecord> temporalRecordDataset) {

    // join datasets by key
    Dataset<Tuple2<BasicRecord, LocationRecord>> joined1 =
        basicRecordDataset.joinWith(
            locationRecordDataset,
            basicRecordDataset.col("id").equalTo(locationRecordDataset.col("id")));

    Dataset<Tuple2<Tuple2<BasicRecord, LocationRecord>, MultiTaxonRecord>> joined2 =
        joined1.joinWith(
            multiTaxonRecordDataset,
            joined1.col("_1.id").equalTo(multiTaxonRecordDataset.col("id")));

    Dataset<Tuple2<Tuple2<Tuple2<BasicRecord, LocationRecord>, MultiTaxonRecord>, TemporalRecord>>
        finalJoined =
            joined2.joinWith(
                temporalRecordDataset,
                joined2.col("_1._1.id").equalTo(temporalRecordDataset.col("id")));

    Dataset<FlatJoinedRecord> flattened =
        finalJoined.map(
            (MapFunction<
                    Tuple2<
                        Tuple2<Tuple2<BasicRecord, LocationRecord>, MultiTaxonRecord>,
                        TemporalRecord>,
                    FlatJoinedRecord>)
                row -> {
                  BasicRecord basic = row._1()._1()._1();
                  LocationRecord location = row._1()._1()._2();
                  MultiTaxonRecord multi = row._1()._2();
                  TemporalRecord temporal = row._2();

                  return new FlatJoinedRecord(basic, location, multi, temporal);
                },
            Encoders.bean(FlatJoinedRecord.class));

    return flattened.map(
        (MapFunction<FlatJoinedRecord, OccurrenceHdfsRecord>)
            flatJoinedRecord ->
                OccurrenceHdfsRecordConverter.builder()
                    .basicRecord(flatJoinedRecord.getBasic())
                    .locationRecord(flatJoinedRecord.getLocation())
                    .temporalRecord(flatJoinedRecord.getTemporal())
                    .multiTaxonRecord(flatJoinedRecord.getMultiTaxon())
                    .build()
                    .convert(),
        Encoders.bean(OccurrenceHdfsRecord.class));
  }

  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  public static class FlatJoinedRecord implements java.io.Serializable {
    private BasicRecord basic;
    private LocationRecord location;
    private MultiTaxonRecord multiTaxon;
    private TemporalRecord temporal;
  }
}
