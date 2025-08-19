package org.gbif.pipelines.interpretation.transform;

import java.io.Serializable;
import java.time.Instant;
import java.util.Optional;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.core.IdentifierInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;

public class IdentifierTransform implements Serializable {

  public static Optional<IdentifierRecord> convert(ExtendedRecord source, String datasetKey) {
    return Interpretation.from(source)
        .to(
            lr ->
                IdentifierRecord.newBuilder()
                    .setId(lr.getId())
                    .setFirstLoaded(Instant.now().toEpochMilli())
                    .build())
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(IdentifierInterpreter.interpretInternalId(datasetKey))
        .getOfNullable();
  }
}
