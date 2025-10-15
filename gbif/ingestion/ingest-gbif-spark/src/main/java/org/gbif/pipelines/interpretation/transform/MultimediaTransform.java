package org.gbif.pipelines.interpretation.transform;

import static org.gbif.pipelines.core.utils.ModelUtils.hasExtension;
import static org.gbif.pipelines.core.utils.ModelUtils.hasValueNullAware;

import java.io.Serializable;
import java.time.Instant;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.extension.MultimediaInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;

@Builder
@AllArgsConstructor
public class MultimediaTransform implements Serializable {

  private transient MultimediaInterpreter multimediaInterpreter;
  private final PipelinesConfig config;

  private MultimediaTransform(PipelinesConfig config) {
    this.config = config;
  }

  public static MultimediaTransform create(PipelinesConfig config) {
    return new MultimediaTransform(config);
  }

  public Optional<MultimediaRecord> convert(ExtendedRecord source) {
    if (multimediaInterpreter == null) {
      multimediaInterpreter =
          MultimediaInterpreter.builder().ordering(config.getDefaultDateFormat()).create();
    }
    return Interpretation.from(source)
        .to(
            er ->
                MultimediaRecord.newBuilder()
                    .setId(er.getId())
                    .setCreated(Instant.now().toEpochMilli())
                    .build())
        .when(
            er ->
                hasExtension(er, Extension.MULTIMEDIA)
                    || hasValueNullAware(er, DwcTerm.associatedMedia))
        .via(multimediaInterpreter::interpret)
        .via(MultimediaInterpreter::interpretAssociatedMedia)
        .getOfNullable();
  }
}
