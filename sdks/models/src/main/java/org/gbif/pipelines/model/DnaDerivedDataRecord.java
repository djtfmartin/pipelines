package org.gbif.pipelines.model;

import java.util.List;

public interface DnaDerivedDataRecord extends Record {

  /**
   * @return list of DNA derived data items
   */
  List<DnaDerivedData> getDnaDerivedDataItems();

  /**
   * @param items the list of DNA derived data items to set
   */
  void setDnaDerivedDataItems(List<DnaDerivedData> items);
}
