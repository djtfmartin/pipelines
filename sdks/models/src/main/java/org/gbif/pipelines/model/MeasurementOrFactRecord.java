package org.gbif.pipelines.model;

import java.util.List;

public interface MeasurementOrFactRecord extends Record, Issues {

  List<MeasurementOrFact> getMeasurementOrFactItems();

  void setMeasurementOrFactItems(List<MeasurementOrFact> measurementOrFactItems);
}
