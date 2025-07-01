package org.gbif.pipelines.core.interpreters.model;

import java.util.List;

public interface AudubonRecord extends Record, Issues {

  List<Audubon> getAudubonItems();

  void setAudubonItems(List<Audubon> audubonItems);
}
