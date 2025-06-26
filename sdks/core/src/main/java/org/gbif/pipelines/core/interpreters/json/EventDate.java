package org.gbif.pipelines.core.interpreters.json;


import lombok.Builder;
import lombok.Data;

/**
 * http://rs.tdwg.org/dwc/terms/eventDate
 */
@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class EventDate {
    private String gte;
    private String lte;
}