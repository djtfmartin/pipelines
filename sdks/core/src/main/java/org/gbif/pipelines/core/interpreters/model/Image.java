package org.gbif.pipelines.core.interpreters.model;

public interface Image extends Record {

    String getIdentifier();
    void setIdentifier(String identifier);

    String getReferences();
    void setReferences(String references);

    String getTitle();
    void setTitle(String title);

    String getDescription();
    void setDescription(String description);

    String getSpatial();
    void setSpatial(String spatial);

    Double getLatitude();
    void setLatitude(Double latitude);

    Double getLongitude();
    void setLongitude(Double longitude);

    String getFormat();
    void setFormat(String format);

    String getCreated();
    void setCreated(String created);

    String getCreator();
    void setCreator(String creator);

    String getContributor();
    void setContributor(String contributor);

    String getPublisher();
    void setPublisher(String publisher);

    String getAudience();
    void setAudience(String audience);

    String getLicense();
    void setLicense(String license);

    String getRights();
    void setRights(String rights);

    String getRightsHolder();
    void setRightsHolder(String rightsHolder);

    String getDatasetId();
    void setDatasetId(String datasetId);
}
