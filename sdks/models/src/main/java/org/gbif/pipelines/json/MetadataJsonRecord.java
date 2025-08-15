package org.gbif.pipelines.json;

import java.util.List;
import lombok.Builder;
import lombok.Data;

/** Metadata information for a dataset. */
@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class MetadataJsonRecord {
  private String datasetKey;
  private String datasetTitle;
  private String installationKey;
  private String hostingOrganizationKey;
  private String endorsingNodeKey;
  private String publisherTitle;
  private String license;
  private String protocol;
  private String publishingCountry;
  private String datasetPublishingCountry;
  private String publishingOrganizationKey;
  private List<String> networkKeys;
  private String projectId;
  private String programmeAcronym;
}
