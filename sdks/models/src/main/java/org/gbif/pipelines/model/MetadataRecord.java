package org.gbif.pipelines.model;

import java.util.List;

public interface MetadataRecord extends Record, Issues {

  Long getLastCrawled();

  void setLastCrawled(Long lastCrawled);

  String getDatasetKey();

  void setDatasetKey(String datasetKey);

  Integer getCrawlId();

  void setCrawlId(Integer crawlId);

  String getDatasetTitle();

  void setDatasetTitle(String datasetTitle);

  String getInstallationKey();

  void setInstallationKey(String installationKey);

  String getPublisherTitle();

  void setPublisherTitle(String publisherTitle);

  String getPublishingOrganizationKey();

  void setPublishingOrganizationKey(String publishingOrganizationKey);

  String getEndorsingNodeKey();

  void setEndorsingNodeKey(String endorsingNodeKey);

  String getProjectId();

  void setProjectId(String projectId);

  String getProgrammeAcronym();

  void setProgrammeAcronym(String programmeAcronym);

  String getProtocol();

  void setProtocol(String protocol);

  String getLicense();

  void setLicense(String license);

  String getDatasetPublishingCountry();

  void setDatasetPublishingCountry(String datasetPublishingCountry);

  String getHostingOrganizationKey();

  void setHostingOrganizationKey(String hostingOrganizationKey);

  List<String> getNetworkKeys();

  void setNetworkKeys(List<String> networkKeys);

  List<MachineTag> getMachineTags();

  void setMachineTags(List<MachineTag> machineTags);
}
