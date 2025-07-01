package org.gbif.pipelines.core.interpreters.model;

import java.util.List;

public interface AmplificationRecord extends Record, Issues {

  List<Amplification> getAmplificationItems();

  void setAmplificationItems(List<Amplification> amplificationItems);
}
