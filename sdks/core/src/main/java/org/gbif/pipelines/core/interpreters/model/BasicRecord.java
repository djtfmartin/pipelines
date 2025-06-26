package org.gbif.pipelines.core.interpreters.model;

import java.util.List;

public interface BasicRecord extends Record {

    String getCoreId();
    void setCoreId(String coreId);

    Long getCreated();
    void setCreated(Long created);

    String getBasisOfRecord();
    void setBasisOfRecord(String basisOfRecord);

    VocabularyConcept getSex();
    void setSex(VocabularyConcept sex);

    VocabularyConcept getLifeStage();
    void setLifeStage(VocabularyConcept lifeStage);

    VocabularyConcept getEstablishmentMeans();
    void setEstablishmentMeans(VocabularyConcept establishmentMeans);

    VocabularyConcept getDegreeOfEstablishment();
    void setDegreeOfEstablishment(VocabularyConcept degreeOfEstablishment);

    VocabularyConcept getPathway();
    void setPathway(VocabularyConcept pathway);

    Integer getIndividualCount();
    void setIndividualCount(Integer individualCount);

    List<VocabularyConcept> getTypeStatus();
    void setTypeStatus(List<VocabularyConcept> typeStatus);

    String getTypifiedName();
    void setTypifiedName(String typifiedName);

    Double getSampleSizeValue();
    void setSampleSizeValue(Double sampleSizeValue);

    String getSampleSizeUnit();
    void setSampleSizeUnit(String sampleSizeUnit);

    Double getOrganismQuantity();
    void setOrganismQuantity(Double organismQuantity);

    String getOrganismQuantityType();
    void setOrganismQuantityType(String organismQuantityType);

    Double getRelativeOrganismQuantity();
    void setRelativeOrganismQuantity(Double relativeOrganismQuantity);

    String getReferences();
    void setReferences(String references);

    String getLicense();
    void setLicense(String license);

    List<AgentIdentifier> getIdentifiedByIds();
    void setIdentifiedByIds(List<AgentIdentifier> identifiedByIds);

    List<String> getIdentifiedBy();
    void setIdentifiedBy(List<String> identifiedBy);

    List<AgentIdentifier> getRecordedByIds();
    void setRecordedByIds(List<AgentIdentifier> recordedByIds);

    List<String> getRecordedBy();
    void setRecordedBy(List<String> recordedBy);

    String getOccurrenceStatus();
    void setOccurrenceStatus(String occurrenceStatus);

    List<String> getDatasetID();
    void setDatasetID(List<String> datasetID);

    List<String> getDatasetName();
    void setDatasetName(List<String> datasetName);

    List<String> getOtherCatalogNumbers();
    void setOtherCatalogNumbers(List<String> otherCatalogNumbers);

    List<String> getPreparations();
    void setPreparations(List<String> preparations);

    List<String> getSamplingProtocol();
    void setSamplingProtocol(List<String> samplingProtocol);

    List<String> getProjectId();
    void setProjectId(List<String> projectId);

    GeologicalContext getGeologicalContext();
    void setGeologicalContext(GeologicalContext geologicalContext);

    Boolean getIsSequenced();
    void setIsSequenced(Boolean isSequenced);

    List<String> getAssociatedSequences();
    void setAssociatedSequences(List<String> associatedSequences);

    IssueRecord getIssues();
    void setIssues(IssueRecord issues);
}
