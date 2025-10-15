package org.gbif.pipelines.interpretation.transform;

import static org.gbif.pipelines.core.utils.ModelUtils.hasExtension;

import java.io.Serializable;
import java.time.Instant;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.extension.DnaDerivedDataInterpreter;
import org.gbif.pipelines.io.avro.DnaDerivedDataRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DnDerivedDataTransform implements Serializable {

  private transient DnaDerivedDataInterpreter dnaDerivedDataInterpreter;

  public static DnDerivedDataTransform create() {
    return new DnDerivedDataTransform();
  }

  public Optional<DnaDerivedDataRecord> convert(ExtendedRecord source) {
    if (dnaDerivedDataInterpreter == null) {
      dnaDerivedDataInterpreter = DnaDerivedDataInterpreter.builder().create();
    }

    return Interpretation.from(source)
        .to(
            er ->
                DnaDerivedDataRecord.newBuilder()
                    .setId(er.getId())
                    .setCreated(Instant.now().toEpochMilli())
                    .build())
        .when(er -> hasExtension(er, Extension.DNA_DERIVED_DATA))
        .via(dnaDerivedDataInterpreter::interpret)
        .getOfNullable();
  }
}
