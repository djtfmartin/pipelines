package org.gbif.pipelines.core.interpreters.model;

import java.util.List;

public interface MeasurementOrFactRecord extends  Record {

    List<MeasurementOrFact> getMeasurementOrFactItems();
    void setMeasurementOrFactItems(List<MeasurementOrFact> measurementOrFactItems);
}
