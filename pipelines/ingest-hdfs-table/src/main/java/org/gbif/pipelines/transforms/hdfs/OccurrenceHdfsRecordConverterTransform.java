package org.gbif.pipelines.transforms.hdfs;

import java.io.Serializable;

import org.gbif.pipelines.core.converters.MultimediaConverter;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.transforms.hdfs.converters.OccurrenceHdfsRecordConverter;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import lombok.AllArgsConstructor;
import lombok.NonNull;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_HDFS_COUNT;

/**
 * Beam level transformation for Occurrence HDFS Downloads Table. The transformation consumes objects, which classes
 * were generated from avro schema files and converts into json string object
 *
 * <p>
 * Example:
 * <p>
 *
 * <pre>{@code
 *
 * final TupleTag<ExtendedRecord> erTag = new TupleTag<ExtendedRecord>() {};
 * final TupleTag<BasicRecord> brTag = new TupleTag<BasicRecord>() {};
 * final TupleTag<TemporalRecord> trTag = new TupleTag<TemporalRecord>() {};
 * final TupleTag<LocationRecord> lrTag = new TupleTag<LocationRecord>() {};
 * final TupleTag<TaxonRecord> txrTag = new TupleTag<TaxonRecord>() {};
 * final TupleTag<MultimediaRecord> mrTag = new TupleTag<MultimediaRecord>() {};
 * final TupleTag<ImageRecord> irTag = new TupleTag<ImageRecord>() {};
 * final TupleTag<AudubonRecord> arTag = new TupleTag<AudubonRecord>() {};
 * final TupleTag<MeasurementOrFactRecord> mfrTag = new TupleTag<MeasurementOrFactRecord>() {};
 *
 * PCollectionView<MetadataRecord> metadataView = ...
 * PCollection<KV<String, ExtendedRecord>> verbatimCollection = ...
 * PCollection<KV<String, BasicRecord>> basicCollection = ...
 * PCollection<KV<String, TemporalRecord>> temporalCollection = ...
 * PCollection<KV<String, LocationRecord>> locationCollection = ...
 * PCollection<KV<String, TaxonRecord>> taxonCollection = ...
 * PCollection<KV<String, MultimediaRecord>> multimediaCollection = ...
 * PCollection<KV<String, ImageRecord>> imageCollection = ...
 * PCollection<KV<String, AudubonRecord>> audubonCollection = ...
 * PCollection<KV<String, MeasurementOrFactRecord>> measurementCollection = ...
 *
 * OccurrenceHdfsRecord record = OccurrenceHdfsRecordConverter.toOccurrenceHdfsRecord(mdr, br, tr, lr, txr, mmr, mfr, er);
 *
 *  c.output(record);
 * }</pre>
 */
@AllArgsConstructor(staticName = "create")
public class OccurrenceHdfsRecordConverterTransform implements Serializable {

  private static final long serialVersionUID = 4605359346756029671L;

  // Core
  @NonNull
  private final TupleTag<ExtendedRecord> erTag;
  @NonNull
  private final TupleTag<BasicRecord> brTag;
  @NonNull
  private final TupleTag<TemporalRecord> trTag;
  @NonNull
  private final TupleTag<LocationRecord> lrTag;
  @NonNull
  private final TupleTag<TaxonRecord> txrTag;
  // Extension
  @NonNull
  private final TupleTag<MultimediaRecord> mrTag;
  @NonNull
  private final TupleTag<ImageRecord> irTag;
  @NonNull
  private final TupleTag<AudubonRecord> arTag;
  @NonNull
  private final TupleTag<MeasurementOrFactRecord> mfrTag;

  @NonNull
  private final PCollectionView<MetadataRecord> metadataView;

  public SingleOutput<KV<String, CoGbkResult>, OccurrenceHdfsRecord> converter() {

    DoFn<KV<String, CoGbkResult>, OccurrenceHdfsRecord> fn = new DoFn<KV<String, CoGbkResult>, OccurrenceHdfsRecord>() {

      private final Counter counter = Metrics.counter(OccurrenceHdfsRecordConverterTransform.class, AVRO_TO_HDFS_COUNT);

      @ProcessElement
      public void processElement(ProcessContext c) {
        CoGbkResult v = c.element().getValue();
        String k = c.element().getKey();

        // Core
        MetadataRecord mdr = c.sideInput(metadataView);
        ExtendedRecord er = v.getOnly(erTag, ExtendedRecord.newBuilder().setId(k).build());
        BasicRecord br = v.getOnly(brTag, BasicRecord.newBuilder().setId(k).build());
        TemporalRecord tr = v.getOnly(trTag, TemporalRecord.newBuilder().setId(k).build());
        LocationRecord lr = v.getOnly(lrTag, LocationRecord.newBuilder().setId(k).build());
        TaxonRecord txr = v.getOnly(txrTag, TaxonRecord.newBuilder().setId(k).build());
        // Extension
        MultimediaRecord mr = v.getOnly(mrTag, MultimediaRecord.newBuilder().setId(k).build());
        ImageRecord ir = v.getOnly(irTag, ImageRecord.newBuilder().setId(k).build());
        AudubonRecord ar = v.getOnly(arTag, AudubonRecord.newBuilder().setId(k).build());
        MeasurementOrFactRecord mfr = v.getOnly(mfrTag, MeasurementOrFactRecord.newBuilder().setId(k).build());

        MultimediaRecord mmr = MultimediaConverter.merge(mr, ir, ar);
        OccurrenceHdfsRecord record = OccurrenceHdfsRecordConverter.toOccurrenceHdfsRecord(br, mdr, tr, lr, txr, mmr, mfr, er);

        c.output(record);

        counter.inc();
      }
    };

    return ParDo.of(fn).withSideInputs(metadataView);
  }

}
