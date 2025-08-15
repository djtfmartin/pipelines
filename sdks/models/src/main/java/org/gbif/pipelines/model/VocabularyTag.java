package org.gbif.pipelines.model;

public interface VocabularyTag {
  String getName();

  String getValue();

  void setName(String key);

  void setValue(String value);
}
