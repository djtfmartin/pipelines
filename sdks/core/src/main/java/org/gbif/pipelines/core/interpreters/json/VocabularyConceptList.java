package org.gbif.pipelines.core.interpreters.json;


import lombok.Builder;
import lombok.Data;
import java.util.List;

@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class VocabularyConceptList {
    private List<String> concepts;
    private List<String> lineage;
}