package org.gbif.pipelines.core.interpreters.json;

import lombok.Builder;
import lombok.Data;
import java.util.List;

@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class GeologicalContext {
    private VocabularyConcept earliestEonOrLowestEonothem;
    private VocabularyConcept latestEonOrHighestEonothem;
    private VocabularyConcept earliestEraOrLowestErathem;
    private VocabularyConcept latestEraOrHighestErathem;
    private VocabularyConcept earliestPeriodOrLowestSystem;
    private VocabularyConcept latestPeriodOrHighestSystem;
    private VocabularyConcept earliestEpochOrLowestSeries;
    private VocabularyConcept latestEpochOrHighestSeries;
    private VocabularyConcept earliestAgeOrLowestStage;
    private VocabularyConcept latestAgeOrHighestStage;
    private String lowestBiostratigraphicZone;
    private String highestBiostratigraphicZone;
    private String group;
    private String formation;
    private String member;
    private String bed;
    private GeologicalRange range;
    private List<String> lithostratigraphy;
    private List<String> biostratigraphy;
}
