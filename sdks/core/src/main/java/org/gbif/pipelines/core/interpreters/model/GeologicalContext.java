package org.gbif.pipelines.core.interpreters.model;

public interface GeologicalContext {
  void setBed(String s);

  void setEarliestAgeOrLowestStage(VocabularyConcept vocabularyConcept);

  void setEarliestEonOrLowestEonothem(VocabularyConcept vocabularyConcept);

  void setEarliestEpochOrLowestSeries(VocabularyConcept vocabularyConcept);

  void setEarliestEraOrLowestErathem(VocabularyConcept vocabularyConcept);

  void setEarliestPeriodOrLowestSystem(VocabularyConcept vocabularyConcept);

  void setEndAge(Float end);

  void setFormation(String s);

  void setGroup(String s);

  void setHighestBiostratigraphicZone(String s);

  void setLatestAgeOrHighestStage(VocabularyConcept vocabularyConcept);

  void setLatestEonOrHighestEonothem(VocabularyConcept vocabularyConcept);

  void setLatestEpochOrHighestSeries(VocabularyConcept vocabularyConcept);

  void setLatestEraOrHighestErathem(VocabularyConcept vocabularyConcept);

  void setLatestPeriodOrHighestSystem(VocabularyConcept vocabularyConcept);

  void setLowestBiostratigraphicZone(String s);

  void setMember(String s);

  void setStartAge(Float start);

  String getLowestBiostratigraphicZone();

  String getHighestBiostratigraphicZone();

  String getGroup();

  String getFormation();

  String getMember();

  String getBed();

  VocabularyConcept getEarliestEonOrLowestEonothem();

  VocabularyConcept getLatestEonOrHighestEonothem();

  VocabularyConcept getEarliestEraOrLowestErathem();

  VocabularyConcept getLatestEraOrHighestErathem();

  VocabularyConcept getEarliestPeriodOrLowestSystem();

  VocabularyConcept getLatestPeriodOrHighestSystem();

  VocabularyConcept getEarliestEpochOrLowestSeries();

  VocabularyConcept getLatestEpochOrHighestSeries();

  VocabularyConcept getEarliestAgeOrLowestStage();

  VocabularyConcept getLatestAgeOrHighestStage();

  Float getStartAge();

  Float getEndAge();
}
