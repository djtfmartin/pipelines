package org.gbif.pipelines.core.interpreters.json;

import lombok.Builder;
import lombok.Data;
import java.util.List;

@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class VocabularyConcept {
    private String concept;
    private List<String> lineage;
}
