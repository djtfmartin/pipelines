package org.gbif.pipelines.core.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class AirflowConfig implements Serializable {
  public String user;
  public String pass;
  public String address;
  public int apiCheckDelaySec = 5;

  // default dag names set here - these can be overridden in config
  public String fragmenterDag = "gbif_pipelines_verbatim_fragmenter_dag";
  public String identifierDag = "gbif_pipelines_occurrence_identifiers_dag";
  public String interpretationDag = "gbif_pipelines_occurrence_interpretation_dag";
  public String tabloBuildDag = "gbif_pipelines_occurrence_hdfs_view_dag";
  public String indexingDag = "gbif_pipelines_occurrence_indexing_dag";
}
