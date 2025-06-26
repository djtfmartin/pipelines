package org.gbif.pipelines.core.interpreters.model;

public interface Multimedia {


    String getType();
    void setType(String type);

    String getFormat();
    void setFormat(String format);

    String getIdentifier();
    void setIdentifier(String identifier);

    String getReferences();
    void setReferences(String references);

    String getTitle();
    void setTitle(String title);

    String getDescription();
    void setDescription(String description);

    String getSource();
    void setSource(String source);

    String getAudience();
    void setAudience(String audience);

    String getCreated();
    void setCreated(String created);

    String getCreator();
    void setCreator(String creator);

    String getContributor();
    void setContributor(String contributor);

    String getPublisher();
    void setPublisher(String publisher);

    String getLicense();
    void setLicense(String license);

    String getRightsHolder();
    void setRightsHolder(String rightsHolder);

    String getDatasetId();
    void setDatasetId(String datasetId);
}
