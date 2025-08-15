package org.gbif.pipelines.model;

public interface Image {

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

  String getCreated();

  void setCreated(String created);
}
