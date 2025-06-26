package org.gbif.pipelines.core.interpreters.json;


import lombok.Builder;
import lombok.Data;
import java.util.List;

@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class ParentJsonRecord {
    // Full-text search field
    private List<String> all;

    // Internal
    private String type;
    private String id;
    private String internalId;
    private String uniqueKey;
    private String firstLoaded;
    private String lastCrawled;
    private String created;
    private Integer crawlId;

    // Metadata
    private MetadataJsonRecord metadata;

    // DerivedMetadata
    private DerivedMetadataRecord derivedMetadata;

    // LocationInherited
    private LocationInheritedRecord locationInherited;

    // TemporalInherited
    private TemporalInheritedRecord temporalInherited;

    // EventInherited
    private EventInheritedRecord eventInherited;

    // Event
    private EventJsonRecord event;

    // Occurrence
    private OccurrenceJsonRecord occurrence;

    // Raw
    private VerbatimRecord verbatim;

    // ES document join field type
    private JoinRecord joinRecord;
}
