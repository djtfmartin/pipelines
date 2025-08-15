package org.gbif.pipelines.model;

import java.util.List;

public interface TaxonRecord extends Record, Issues {

  // parentId
  String getParentId();

  void setParentId(String parentId);

  // datasetKey
  String getDatasetKey();

  void setDatasetKey(String datasetKey);

  // synonym
  Boolean getSynonym();

  void setSynonym(Boolean synonym);

  // usage
  RankedNameWithAuthorship getUsage();

  void setUsage(RankedNameWithAuthorship usage);

  // classification
  List<RankedName> getClassification();

  void setClassification(List<RankedName> classification);

  // acceptedUsage
  RankedNameWithAuthorship getAcceptedUsage();

  void setAcceptedUsage(RankedNameWithAuthorship acceptedUsage);

  //    // nomenclature
  //    Nomenclature getNomenclature();
  //    void setNomenclature(Nomenclature nomenclature);
  //
  //    // diagnostics
  //    Diagnostic getDiagnostics();
  //    void setDiagnostics(Diagnostic diagnostics);
  //
  //    // usageParsedName
  //    ParsedName getUsageParsedName();
  //    void setUsageParsedName(ParsedName usageParsedName);

  // iucnRedListCategoryCode
  String getIucnRedListCategoryCode();

  void setIucnRedListCategoryCode(String code);

  void setDiagnostics(Diagnostics diagnostic);
}
