package org.gbif.pipelines.interpretation.transform;

import static org.gbif.pipelines.core.utils.ModelUtils.hasExtension;

import java.io.Serializable;
import java.time.Instant;
import java.util.Optional;
import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.extension.AudubonInterpreter;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

public class AudubonTransform implements Serializable {

  private final PipelinesConfig config;
  private transient AudubonInterpreter audubonInterpreter;

  private AudubonTransform(PipelinesConfig config) {
    this.config = config;
  }

  public static AudubonTransform create(PipelinesConfig config) {
    return new AudubonTransform(config);
  }

  public Optional<AudubonRecord> convert(ExtendedRecord source) {

    if (audubonInterpreter == null) {
      audubonInterpreter =
          AudubonInterpreter.builder().orderings(config.getDefaultDateFormat()).create();
    }

    return Interpretation.from(source)
        .to(
            er ->
                AudubonRecord.newBuilder()
                    .setId(er.getId())
                    .setCreated(Instant.now().toEpochMilli())
                    .build())
        .when(er -> hasExtension(er, Extension.AUDUBON))
        .via(audubonInterpreter::interpret)
        .getOfNullable();
  }
}
