package org.gbif.pipelines.interpretation.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.gbif.pipelines.core.converters.OccurrenceHdfsRecordConverter;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import scala.Tuple2;

@Slf4j
public class HdfsView implements java.io.Serializable {

  public static Dataset<OccurrenceHdfsRecord> transformToHdfsView(
      Dataset<ExtendedRecord> records,
      MetadataRecord metadataRecord,
      Dataset<IdentifierRecord> identifierRecordDataset,
      Dataset<BasicRecord> basicRecordDataset,
      Dataset<LocationRecord> locationRecordDataset,
      Dataset<MultiTaxonRecord> multiTaxonRecordDataset,
      Dataset<TemporalRecord> temporalRecordDataset,
      Dataset<GrscicollRecord> grscicollDataset) {

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
        joined3 =
            joined2.joinWith(
                temporalRecordDataset,
                joined2.col("_1._1.id").equalTo(temporalRecordDataset.col("id")));

    Dataset<
            Tuple2<
                Tuple2<
                    Tuple2<Tuple2<BasicRecord, LocationRecord>, MultiTaxonRecord>, TemporalRecord>,
                GrscicollRecord>>
        finalJoined =
            joined3.joinWith(
                grscicollDataset, joined3.col("_1._1._1.id").equalTo(grscicollDataset.col("id")));

    return finalJoined.map(
        (MapFunction<
                Tuple2<
                    Tuple2<
                        Tuple2<Tuple2<BasicRecord, LocationRecord>, MultiTaxonRecord>,
                        TemporalRecord>,
                    GrscicollRecord>,
                OccurrenceHdfsRecord>)
            row -> {
              BasicRecord basic = row._1()._1()._1()._1();
              LocationRecord location = row._1()._1()._1()._2();
              MultiTaxonRecord multi = row._1()._1()._2();
              TemporalRecord temporal = row._1()._2();
              GrscicollRecord grscicollRecord = row._2();

              log.debug("Converting id={}", basic.getId());
              OccurrenceHdfsRecordConverter c =
                  OccurrenceHdfsRecordConverter.builder()
                      .metadataRecord(metadataRecord)
                      .basicRecord(basic)
                      .locationRecord(location)
                      .temporalRecord(temporal)
                      .multiTaxonRecord(multi)
                      .grscicollRecord(grscicollRecord)
                      .build();
              return c.convert();
            },
        Encoders.bean(OccurrenceHdfsRecord.class));
  }
}
