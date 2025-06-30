package org.gbif.pipelines.core.interpreters.model;

import java.util.List;

public interface AudubonRecord extends Record {

    List<Audubon> getAudubonItems();
    void setAudubonItems(List<Audubon> audubonItems);
}
