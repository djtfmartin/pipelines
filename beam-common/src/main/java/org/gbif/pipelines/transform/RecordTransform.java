package org.gbif.pipelines.transform;

import org.gbif.pipelines.io.avro.issue.OccurrenceIssue;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * Common class for a transform record process
 *
 * @param <T> transform "from" class
 * @param <R> transform "to" class
 */
public abstract class RecordTransform<T, R> extends PTransform<PCollection<T>, PCollectionTuple> {

  protected RecordTransform(String stepName) {
    this.stepName = stepName;
  }

  private final String stepName;
  private final TupleTag<KV<String, R>> dataTag = new TupleTag<KV<String, R>>() {};
  private final TupleTag<KV<String, OccurrenceIssue>> issueTag =
      new TupleTag<KV<String, OccurrenceIssue>>() {};

  @Override
  public PCollectionTuple expand(PCollection<T> input) {
    return input.apply(
        stepName, ParDo.of(interpret()).withOutputTags(dataTag, TupleTagList.of(issueTag)));
  }

  public abstract DoFn<T, KV<String, R>> interpret();

  /** @return all data, without and with issues */
  public TupleTag<KV<String, R>> getDataTag() {
    return dataTag;
  }

  /** @return data only with issues */
  public TupleTag<KV<String, OccurrenceIssue>> getIssueTag() {
    return issueTag;
  }

  public abstract RecordTransform<T, R> withAvroCoders(Pipeline pipeline);
}
