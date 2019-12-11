package org.gbif.pipelines.core.interpreters.specific;

import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.BiConsumer;

import org.apache.commons.beanutils.BeanUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.io.avro.AustraliaSpatialRecord;
import org.gbif.pipelines.io.avro.LocationRecord;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.parsers.utils.ModelUtils.extractValue;

/** Interprets the location of a {@link AustraliaSpatialRecord}. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AustraliaSpatialInterpreter {

  public static BiConsumer<LocationRecord, AustraliaSpatialRecord> interpret(KeyValueStore<LatLng, String> kvStore) {
    return (lr, asr) -> {
      try {
        if (lr.getDecimalLongitude() != null && lr.getDecimalLatitude() != null) {
          ObjectMapper om = new ObjectMapper();
          Map<String, String> items = om.readValue(
                  new URL("http://localhost:8080/intersect/" + lr.getDecimalLatitude() + "/" + lr.getDecimalLongitude()),
                  Map.class
          );
          if (items != null && !items.isEmpty()) {
            System.out.println("################# Found sampling....");
            asr.setItems(items);
          } else {
            System.out.println("################# No Response from sampling found");
          }
        }
      } catch (Exception e){
        e.printStackTrace();
      }




//      if (kvStore != null) {
//        try {
//          // Call kv store
//          String json = kvStore.get(new LatLng(lr.getDecimalLatitude(), lr.getDecimalLongitude()));
//
//          // Parse json
//          if (!Strings.isNullOrEmpty(json)) {
//            json = json.substring(11, json.length() - 1);
//            ObjectMapper objectMapper = new ObjectMapper();
//            Map<String, String> map = objectMapper.readValue(json, new TypeReference<HashMap<String, String>>() {});
//            asr.setItems(map);
//          }
//        } catch (NoSuchElementException | NullPointerException | IOException ex) {
//          log.error(ex.getMessage(), ex);
//        }
//      }
    };
  }
}
