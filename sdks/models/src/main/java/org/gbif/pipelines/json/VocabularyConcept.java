package org.gbif.pipelines.json;

import java.util.List;
import lombok.Builder;
import lombok.Data;

@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class VocabularyConcept {
  private String concept;
  private List<String> lineage;
}
