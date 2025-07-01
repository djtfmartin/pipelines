package org.gbif.pipelines.core.interpreters.json;

import java.util.List;
import lombok.Builder;
import lombok.Data;

@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class VocabularyConceptList {
  private List<String> concepts;
  private List<String> lineage;
}
