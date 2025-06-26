package org.gbif.pipelines.core.interpreters.json;

import lombok.Builder;
import lombok.Data;
import java.util.Map;
import java.util.List;

/**
 * A container for an extended DwC record (core plus extension data for a single record)
 */
@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class VerbatimRecord {

    private Map<String, String> core;
    private String coreId;
    private Map<String, List<Map<String, String>>> extensions;
}
