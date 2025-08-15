package org.gbif.pipelines.model;

public interface GrscicollRecord extends Record, Issues {

  /**
   * @return the institution match, or null if not present
   */
  Match getInstitutionMatch();

  /**
   * @param match the institution match to set
   */
  void setInstitutionMatch(Match match);

  /**
   * @return the collection match, or null if not present
   */
  Match getCollectionMatch();

  /**
   * @param match the collection match to set
   */
  void setCollectionMatch(Match match);
}
