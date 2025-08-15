package org.gbif.pipelines.model;

import java.util.List;

public interface EventCoreRecord extends Record, Issues {

  // parentEventID
  String getParentEventID();

  void setParentEventID(String parentEventID);

  // eventType
  VocabularyConcept getEventType();

  void setEventType(VocabularyConcept eventType);

  // sampleSizeValue
  Double getSampleSizeValue();

  void setSampleSizeValue(Double sampleSizeValue);

  // sampleSizeUnit
  String getSampleSizeUnit();

  void setSampleSizeUnit(String sampleSizeUnit);

  // references
  String getReferences();

  void setReferences(String references);

  // license
  String getLicense();

  void setLicense(String license);

  // datasetID
  List<String> getDatasetID();

  void setDatasetID(List<String> datasetID);

  // datasetName
  List<String> getDatasetName();

  void setDatasetName(List<String> datasetName);

  // samplingProtocol
  List<String> getSamplingProtocol();

  void setSamplingProtocol(List<String> samplingProtocol);

  // parentsLineage
  List<Parent> getParentsLineage();

  void setParentsLineage(List<Parent> parentsLineage);

  // locationID
  String getLocationID();

  void setLocationID(String locationID);
}
