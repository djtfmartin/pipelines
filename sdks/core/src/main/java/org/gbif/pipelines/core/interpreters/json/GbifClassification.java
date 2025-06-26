package org.gbif.pipelines.core.interpreters.json;


import lombok.Builder;
import lombok.Data;
import java.util.List;

/**
 * GBIF taxonomy classification (deprecated).
 */
@Data
@Deprecated
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class GbifClassification {
    private RankedNameWithAuthorship acceptedUsage;
    private List<RankedName> classification;
    private String classificationPath;
    private Diagnostic diagnostics;
    private String kingdom;
    private String kingdomKey;
    private String phylum;
    private String phylumKey;
    private String clazz; // 'class' is a reserved word in Java
    private String classKey;
    private String order;
    private String orderKey;
    private String family;
    private String familyKey;
    private String genus;
    private String genusKey;
    private String species;
    private String speciesKey;
    private Boolean synonym;
    private String taxonID;
    private String taxonConceptID;
    private List<String> taxonKey;
    private RankedNameWithAuthorship usage;
    private ParsedName usageParsedName;
    private String verbatimScientificName;
    private String iucnRedListCategoryCode;
}
