package org.gbif.pipelines.core.interpreters.model;

import org.jetbrains.annotations.NotNull;

public interface EventDate {
   String getGte();
   void setGte(String gte);

   String getLte();
   void setLte(String lte);

   String getInterval();
   void setInterval(String interval);
}
