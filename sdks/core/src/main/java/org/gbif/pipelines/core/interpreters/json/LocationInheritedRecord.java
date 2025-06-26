package org.gbif.pipelines.core.interpreters.json;

import lombok.Builder;

@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class LocationInheritedRecord {

    /** Pipelines identifier */
    private String id;

    /** Id of the parent whose fields are inherited */
    private String inheritedFrom;

    private String countryCode;
    private String stateProvince;
    private Double decimalLatitude;
    private Double decimalLongitude;
}
