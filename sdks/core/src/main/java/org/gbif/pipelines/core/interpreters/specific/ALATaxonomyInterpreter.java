package org.gbif.pipelines.core.interpreters.specific;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.species.SpeciesMatchRequest;
import org.gbif.pipelines.io.avro.ALATaxonRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.rest.client.species.NameUsageMatch;

import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.gbif.pipelines.parsers.utils.ModelUtils.extractValue;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALATaxonomyInterpreter {

    /**
     * Interprets a utils from the taxonomic fields specified in the {@link ExtendedRecord} received.
     */
    public static BiConsumer<ExtendedRecord, ALATaxonRecord> alaTaxonomyInterpreter(
            KeyValueStore<SpeciesMatchRequest, NameUsageMatch> kvStore) {
        return (er, atr) -> {

            System.out.println("Setting ATR ID = " + er.getId() );
            atr.setId(er.getId());
            try {
                String scientificName = extractValue(er, DwcTerm.scientificName);
                ObjectMapper om = new ObjectMapper();
                Map<String, Object> result = om.readValue(new URL("http://localhost:9179/search?q=" + URLEncoder.encode(scientificName, "UTF-8")), Map.class);

                if (result.get("acceptedGuid") != null){
                    System.out.println("[" + er.getId() + "] ##### Success matching : " + scientificName);
                    atr.setCleanName(result.get("cleanName").toString());
                    atr.setAcceptedLsid(result.get("acceptedGuid").toString());
                    atr.setLeft(result.get("left").toString());
                    atr.setRight(result.get("right").toString());
                    atr.setMatchType(result.get("matchType").toString());
                } else {
                    System.out.println("[" + er.getId() + "] ##### FAILED matching : " + scientificName);
                }
            } catch(Exception e){
                e.printStackTrace();
            }
        };
    }

}
