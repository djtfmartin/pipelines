package org.gbif.pipelines.model;

import java.util.Map;

public interface LocationFeatureRecord extends Record {
  void setItems(Map<String, String> map);
}
