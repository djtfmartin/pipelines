package org.gbif.pipelines.core.interpreters.json;


import lombok.Builder;
import lombok.Data;

/**
 * Represents a Measurement or Fact from Darwin Core.
 */
@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class MeasurementOrFactJsonRecord {
    private String measurementID;
    private String measurementType;
    private String measurementValue;
    private String measurementUnit;
    private String measurementAccuracy;
    private String measurementDeterminedBy;
    private String measurementDeterminedDate;
    private String measurementMethod;
    private String measurementRemarks;
}
