package org.gbif.pipelines.model;

import java.util.List;

public interface Match {

  /**
   * @return the match type, or null if not present
   */
  String getMatchType();

  /**
   * @param matchType the match type to set
   */
  void setMatchType(String matchType);

  /**
   * @return the status, or null if not present
   */
  String getStatus();

  /**
   * @param status the status to set
   */
  void setStatus(String status);

  /**
   * @return the list of reasons, or null if not present
   */
  List<String> getReasons();

  /**
   * @param reasons the list of reasons to set
   */
  void setReasons(List<String> reasons);

  /**
   * @return the match key, or null if not present
   */
  String getKey();

  /**
   * @param key the match key to set
   */
  void setKey(String key);
}
