package org.gbif.pipelines.core.interpreters.model;

public interface MeasurementOrFact {

    String getMeasurementID();
    void setMeasurementID(String measurementID);

    String getMeasurementType();
    void setMeasurementType(String measurementType);

    String getMeasurementValue();
    void setMeasurementValue(String measurementValue);

    String getMeasurementUnit();
    void setMeasurementUnit(String measurementUnit);

    String getMeasurementAccuracy();
    void setMeasurementAccuracy(String measurementAccuracy);

    String getMeasurementDeterminedBy();
    void setMeasurementDeterminedBy(String measurementDeterminedBy);

    String getMeasurementDeterminedDate();
    void setMeasurementDeterminedDate(String measurementDeterminedDate);

    String getMeasurementMethod();
    void setMeasurementMethod(String measurementMethod);

    String getMeasurementRemarks();
    void setMeasurementRemarks(String measurementRemarks);
}
