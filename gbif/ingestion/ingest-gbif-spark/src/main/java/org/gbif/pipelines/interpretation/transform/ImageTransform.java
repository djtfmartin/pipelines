package org.gbif.pipelines.interpretation.transform;

import static org.gbif.pipelines.core.utils.ModelUtils.hasExtension;

import java.io.Serializable;
import java.time.Instant;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.extension.ImageInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.ImageRecord;

@Builder
@AllArgsConstructor
public class ImageTransform implements Serializable {

  private transient ImageInterpreter imageInterpreter;
  private final PipelinesConfig config;

  private ImageTransform(PipelinesConfig config) {
    this.config = config;
  }

  public static ImageTransform create(PipelinesConfig config) {
    return new ImageTransform(config);
  }

  public Optional<ImageRecord> convert(ExtendedRecord source) {
    if (imageInterpreter == null) {
      imageInterpreter =
          ImageInterpreter.builder().orderings(config.getDefaultDateFormat()).create();
    }
    return Interpretation.from(source)
        .to(
            er ->
                ImageRecord.newBuilder()
                    .setId(er.getId())
                    .setCreated(Instant.now().toEpochMilli())
                    .build())
        .when(er -> hasExtension(er, Extension.IMAGE))
        .via(imageInterpreter::interpret)
        .getOfNullable();
  }
}
