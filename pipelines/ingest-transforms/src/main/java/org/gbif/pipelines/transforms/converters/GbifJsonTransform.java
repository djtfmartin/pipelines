package org.gbif.pipelines.transforms.converters;

import java.io.Serializable;

import org.gbif.pipelines.core.converters.GbifJsonConverter;
import org.gbif.pipelines.core.converters.MultimediaConverter;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

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

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_JSON_COUNT;

/**
 * Beam level transformation for the ES output json. The transformation consumes objects, which classes were generated
 * from avro schema files and converts into json string object
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
 * SingleOutput<KV<String, CoGbkResult>, String> gbifJsonDoFn =
 *     GbifJsonTransform.create(erTag, brTag, trTag, lrTag, txrTag, mrTag, irTag, arTag, mfrTag, metadataView)
 *         .converter();
 *
 * PCollection<String> jsonCollection =
 *     KeyedPCollectionTuple
 *         // Core
 *         .of(brTag, basicCollection)
 *         .and(trTag, temporalCollection)
 *         .and(lrTag, locationCollection)
 *         .and(txrTag, taxonCollection)
 *         // Extension
 *         .and(mrTag, multimediaCollection)
 *         .and(irTag, imageCollection)
 *         .and(arTag, audubonCollection)
 *         .and(mfrTag, measurementCollection)
 *         // Raw
 *         .and(erTag, verbatimCollection)
 *         // Apply
 *         .apply("Grouping objects", CoGroupByKey.create())
 *         .apply("Merging to json", gbifJsonDoFn);
 * }</pre>
 */
@AllArgsConstructor(staticName = "create")
public class GbifJsonTransform implements Serializable {

  private static final long serialVersionUID = 1279313931024806169L;

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

  public SingleOutput<KV<String, CoGbkResult>, String> converter() {

    DoFn<KV<String, CoGbkResult>, String> fn = new DoFn<KV<String, CoGbkResult>, String>() {

      private final Counter counter = Metrics.counter(GbifJsonTransform.class, AVRO_TO_JSON_COUNT);

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
        String json = GbifJsonConverter.toStringJson(mdr, br, tr, lr, txr, mmr, mfr, er);

        c.output(json);

        counter.inc();
      }
    };

    return ParDo.of(fn).withSideInputs(metadataView);
  }
}
