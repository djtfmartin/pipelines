package org.gbif.pipelines.interpretation.transform;

import java.time.Instant;
import java.util.Optional;
import java.util.function.BiConsumer;
import lombok.Builder;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.specific.GbifIdInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.keygen.HBaseLockingKey;

@Builder
public class GbifIdTransform {

  private final String absentName;
  private final boolean isTripletValid;
  private final boolean isOccurrenceIdValid;
  private final boolean useExtendedRecordId;
  private final boolean generateIdIfAbsent;
  private final BiConsumer<ExtendedRecord, IdentifierRecord> gbifIdFn;
  private final HBaseLockingKey keygenService;

  public Optional<IdentifierRecord> convert(ExtendedRecord source) {

    // initialise a new IdentifierRecord with id from ExtendedRecord and current timestamp
    IdentifierRecord ir =
        IdentifierRecord.newBuilder()
            .setId(source.getId())
            .setFirstLoaded(Instant.now().toEpochMilli())
            .build();

    return Interpretation.from(source)
        .to(ir)
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(GbifIdInterpreter.interpretGbifId(keygenService, false, true, false, false, null))
        .getOfNullable();
  }
}
