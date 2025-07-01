package org.gbif.pipelines.core.interpreters.model;

import java.util.Collection;
import java.util.List;

public interface VocabularyConcept {

  String getConcept();

  Collection<VocabularyTag> getTags();

  List<String> getLineage();

  void setConcept(String conceptName);

  void setLineage(List<String> strings);

  void setTags(List<VocabularyTag> vocabularyTags);
}
