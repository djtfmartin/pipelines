package org.gbif.pipelines.interpretation.transform;

import java.time.Instant;
import java.util.Optional;
import lombok.Builder;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.grscicoll.GrscicollLookupRequest;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.core.GrscicollInterpreter;
import org.gbif.pipelines.interpretation.transform.utils.GrscicollLookupKvStoreFactory;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.rest.client.grscicoll.GrscicollLookupResponse;

@Builder
public class GrscicollTransform implements java.io.Serializable {

  private PipelinesConfig config;

  public Optional<GrscicollRecord> convert(ExtendedRecord source, MetadataRecord mdr) {

    KeyValueStore<GrscicollLookupRequest, GrscicollLookupResponse> kvStore =
        GrscicollLookupKvStoreFactory.getKvStore(config);

    return Interpretation.from(source)
        .to(GrscicollRecord.newBuilder().setCreated(Instant.now().toEpochMilli()).build())
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(GrscicollInterpreter.grscicollInterpreter(kvStore, mdr))
        .skipWhen(gr -> gr.getId() == null)
        .getOfNullable();
  }
}
