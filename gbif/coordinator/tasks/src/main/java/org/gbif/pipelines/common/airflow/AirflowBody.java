package org.gbif.pipelines.common.airflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;
import org.gbif.pipelines.common.process.AirflowConfFactory;

@Data
@Builder
public class AirflowBody {

  @JsonProperty("dag_run_id")
  private final String dagRunId;

  private final AirflowConfFactory.Conf conf;
}
