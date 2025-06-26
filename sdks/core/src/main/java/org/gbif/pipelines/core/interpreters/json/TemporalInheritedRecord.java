package org.gbif.pipelines.core.interpreters.json;

import lombok.Builder;

@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class TemporalInheritedRecord {

    /** Pipelines identifier */
    private String id;

    /** Id of the parent whose fields are inherited */
    private String inheritedFrom;

    /** http://rs.tdwg.org/dwc/terms/year */
    private Integer year;

    /** http://rs.tdwg.org/dwc/terms/month */
    private Integer month;

    /** http://rs.tdwg.org/dwc/terms/day */
    private Integer day;
}
