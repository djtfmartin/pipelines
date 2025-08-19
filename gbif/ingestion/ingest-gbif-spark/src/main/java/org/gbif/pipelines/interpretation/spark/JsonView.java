package org.gbif.pipelines.interpretation.spark;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.gbif.pipelines.core.converters.OccurrenceJsonConverter;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;
import scala.Tuple2;

public class JsonView {

  public static Dataset<OccurrenceJsonRecord> transformToJsonView(
      Dataset<ExtendedRecord> records,
      MetadataRecord metadataRecord,
      Dataset<IdentifierRecord> identifierRecordDataset,
      Dataset<BasicRecord> basicRecordDataset,
      Dataset<LocationRecord> locationRecordDataset,
      Dataset<MultiTaxonRecord> multiTaxonRecordDataset,
      Dataset<TemporalRecord> temporalRecordDataset,
      Dataset<GrscicollRecord> grscicollDataset) {

    Dataset<Tuple2<ExtendedRecord, BasicRecord>> join1 =
        records.joinWith(
            basicRecordDataset,
                records.col("id").equalTo(basicRecordDataset.col("id")));

    // join datasets by key
    Dataset<Tuple2<Tuple2<ExtendedRecord, BasicRecord>, LocationRecord>> join2 =
        join1.joinWith(
            locationRecordDataset,
                join1.col("_1.id").equalTo(locationRecordDataset.col("id")));

    Dataset<Tuple2<Tuple2<Tuple2<ExtendedRecord, BasicRecord>, LocationRecord>, MultiTaxonRecord>>
        join3 =
            join2.joinWith(
                multiTaxonRecordDataset,
                    join2.col("_1._1.id").equalTo(multiTaxonRecordDataset.col("id")));

    Dataset<
            Tuple2<
                Tuple2<
                    Tuple2<Tuple2<ExtendedRecord, BasicRecord>, LocationRecord>, MultiTaxonRecord>,
                TemporalRecord>>
        join4 =
            join3.joinWith(
                temporalRecordDataset,
                    join3.col("_1._1._1.id").equalTo(temporalRecordDataset.col("id")));

    Dataset<
            Tuple2<
                Tuple2<
                    Tuple2<
                        Tuple2<Tuple2<ExtendedRecord, BasicRecord>, LocationRecord>,
                        MultiTaxonRecord>,
                    TemporalRecord>,
                GrscicollRecord>>
        join5 =
            join4.joinWith(
                grscicollDataset, join4.col("_1._1._1._1.id").equalTo(grscicollDataset.col("id")));

    Dataset<
            Tuple2<
                Tuple2<
                    Tuple2<
                        Tuple2<
                            Tuple2<Tuple2<ExtendedRecord, BasicRecord>, LocationRecord>,
                            MultiTaxonRecord>,
                        TemporalRecord>,
                    GrscicollRecord>,
                IdentifierRecord>>
        join6 =
            join5.joinWith(
                identifierRecordDataset,
                    join5.col("_1._1._1._1._1.id").equalTo(identifierRecordDataset.col("id")));

    return join6.map(
        (MapFunction<
                Tuple2<
                    Tuple2<
                        Tuple2<
                            Tuple2<
                                Tuple2<Tuple2<ExtendedRecord, BasicRecord>, LocationRecord>,
                                MultiTaxonRecord>,
                            TemporalRecord>,
                        GrscicollRecord>,
                    IdentifierRecord>,
                OccurrenceJsonRecord>)
            row -> {

              ExtendedRecord extended = row._1()._1()._1()._1()._1()._1();
              BasicRecord basic = row._1()._1()._1()._1()._1()._2();
              LocationRecord location = row._1()._1()._1()._1()._2();
              MultiTaxonRecord multi = row._1()._1()._1()._2();
              TemporalRecord temporal = row._1()._1()._2();
              GrscicollRecord grscicollRecord = row._1()._2();
              IdentifierRecord identifierRecord = row._2();

              OccurrenceJsonConverter c =
                  OccurrenceJsonConverter.builder()
                      .verbatim(extended)
                      .metadata(metadataRecord)
                      .basic(basic)
                      .location(location)
                      .temporal(temporal)
                      .multiTaxon(multi)
                      .grscicoll(grscicollRecord)
                      .identifier(identifierRecord)
                      .build();

              return c.convert();
            },
        Encoders.bean(OccurrenceJsonRecord.class));
  }
}
