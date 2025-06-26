package org.gbif.pipelines.core.interpreters.model;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Set;

public interface TaxonRecord extends Record {

    // id
    String getId();
    void setId(String id);

    // coreId
    String getCoreId();
    void setCoreId(String coreId);

    // parentId
    String getParentId();
    void setParentId(String parentId);

    // datasetKey
    String getDatasetKey();
    void setDatasetKey(String datasetKey);

    // created
    Long getCreated();
    void setCreated(Long created);

    // synonym
    Boolean getSynonym();
    void setSynonym(Boolean synonym);

    // usage
    RankedNameWithAuthorship getUsage();
    void setUsage(RankedNameWithAuthorship usage);

    // classification
    List<RankedName> getClassification();
    void setClassification(List<RankedName> classification);

    // acceptedUsage
    RankedNameWithAuthorship getAcceptedUsage();
    void setAcceptedUsage(RankedNameWithAuthorship acceptedUsage);

//    // nomenclature
//    Nomenclature getNomenclature();
//    void setNomenclature(Nomenclature nomenclature);
//
//    // diagnostics
//    Diagnostic getDiagnostics();
//    void setDiagnostics(Diagnostic diagnostics);
//
//    // usageParsedName
//    ParsedName getUsageParsedName();
//    void setUsageParsedName(ParsedName usageParsedName);

    // issues
    IssueRecord getIssues();
    void setIssues(IssueRecord issues);

    // iucnRedListCategoryCode
    String getIucnRedListCategoryCode();
    void setIucnRedListCategoryCode(String code);

    void setDiagnostics(Diagnostics diagnostic);
}
