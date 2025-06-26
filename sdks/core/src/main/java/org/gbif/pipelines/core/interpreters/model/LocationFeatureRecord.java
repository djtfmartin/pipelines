package org.gbif.pipelines.core.interpreters.model;

import java.util.Map;

public interface LocationFeatureRecord extends Record {
    void setItems(Map<String, String> map);
}
