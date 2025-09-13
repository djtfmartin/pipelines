/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.pipelines.interpretation.spark;

import static org.gbif.dwc.terms.DwcTerm.*;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.interpretation.transform.MultiTaxonomyTransform;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MultiTaxonRecord;
import scala.Tuple2;

@Slf4j
public class TaxonomyInterpretation {

  /** Interprets the temporal information contained in the extended records. */
  public static Dataset<Tuple2<String, byte[]>> taxonomyTransform(
      PipelinesConfig config,
      SparkSession spark,
      Dataset<ExtendedRecord> source,
      int numPartitions) {

    MultiTaxonomyTransform multiTaxonomyTransform =
        MultiTaxonomyTransform.builder().config(config).build();

    // extract the taxonomy from the extended records
    spark.sparkContext().setJobGroup("taxonomy", "Extract the taxonomy ", true);
    Dataset<RecordWithTaxonomy> recordWithTaxonomy =
        source.map(
            (MapFunction<ExtendedRecord, RecordWithTaxonomy>)
                er -> {
                  Taxonomy taxonomy = Taxonomy.buildFrom(er);
                  return RecordWithTaxonomy.builder()
                      .id(er.getId())
                      .hash(taxonomy.hash())
                      .taxonomy(taxonomy)
                      .build();
                },
            Encoders.bean(RecordWithTaxonomy.class));

    // distinct the classifications to lookup
    spark.sparkContext().setJobGroup("taxonomy", "Distinct classifications", true);
    Dataset<RecordWithTaxonomy> distinctClassifications =
        recordWithTaxonomy.dropDuplicates("hash").repartition(numPartitions);

    // lookup the distinct classifications, and create a dictionary of the results
    spark.sparkContext().setJobGroup("taxonomy", "Lookup the distinct classifications", true);
    Dataset<KeyedMultiTaxonRecord> keyedLocation =
        distinctClassifications.map(
            (MapFunction<RecordWithTaxonomy, KeyedMultiTaxonRecord>)
                taxonomy -> {

                  // HACK - the function takes ExtendedRecord, but we have a Location
                  ExtendedRecord er =
                      ExtendedRecord.newBuilder()
                          .setId("UNUSED_BUT_NECESSARY")
                          .setCoreTerms(taxonomy.getTaxonomy().toCoreTermsMap())
                          .build();

                  // look them up
                  Optional<MultiTaxonRecord> converted = multiTaxonomyTransform.convert(er);
                  if (converted.isPresent()) {
                    return KeyedMultiTaxonRecord.builder()
                        .key(taxonomy.getTaxonomy().hash())
                        .multiTaxonRecord(converted.get())
                        .build();
                  } else {
                    return KeyedMultiTaxonRecord.builder()
                        .key(taxonomy.getTaxonomy().hash())
                        .build(); // TODO: null handling?
                  }
                },
            Encoders.bean(KeyedMultiTaxonRecord.class));

    // join the dictionary back to the source records
    spark.sparkContext().setJobGroup("taxonomy", "Join matches to source records", true);
    return recordWithTaxonomy
        .joinWith(
            keyedLocation,
            recordWithTaxonomy.col("hash").equalTo(keyedLocation.col("key")),
            "left_outer")
        .map(
            (MapFunction<Tuple2<RecordWithTaxonomy, KeyedMultiTaxonRecord>, Tuple2<String, byte[]>>)
                t -> {
                  RecordWithTaxonomy rwl = t._1();
                  KeyedMultiTaxonRecord klr = t._2();

                  MultiTaxonRecord multiTaxonRecord =
                      (klr != null && klr.getMultiTaxonRecord() != null)
                          ? klr.getMultiTaxonRecord()
                          : MultiTaxonRecord.newBuilder().build();

                  multiTaxonRecord.setId(rwl.getId());
                  return Tuple2.apply(rwl.getId(), KryoUtils.serialize(multiTaxonRecord));
                },
            Encoders.tuple(Encoders.STRING(), Encoders.BINARY()));
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class RecordWithTaxonomy {
    private String id;
    private String hash;
    private Taxonomy taxonomy;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class KeyedMultiTaxonRecord {
    private String key;
    private MultiTaxonRecord multiTaxonRecord;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Taxonomy {
    protected String taxonID;
    protected String taxonConceptID;
    protected String scientificNameID;
    protected String scientificName;
    protected String scientificNameAuthorship;
    protected String taxonRank;
    protected String verbatimTaxonRank;
    protected String genericName;
    protected String specificEpithet;
    protected String infraspecificEpithet;
    protected String higherClassification;
    protected String kingdom;
    protected String phylum;
    protected String clazz;
    protected String order;
    protected String superfamily;
    protected String family;
    protected String subfamily;
    protected String tribe;
    protected String subtribe;
    protected String genus;
    protected String subgenus;
    protected String species;
    protected String nomenclaturalCode;

    static Taxonomy buildFrom(ExtendedRecord er) {
      TaxonomyBuilder builder = Taxonomy.builder();

      Arrays.stream(DwcTerm.values())
          .filter(t -> GROUP_TAXON.equals(t.getGroup()) && !t.isClass())
          .forEach(
              term -> {
                String fieldName = term.simpleName(); // e.g., "country"
                String value =
                    er.getCoreTerms()
                        .get(term.qualifiedName()); // or however the ER provides values

                if (value != null) {
                  try {
                    if (fieldName.equals("class")) {
                      builder.clazz(value);
                    } else {
                      Method setter = builder.getClass().getMethod(fieldName, String.class);
                      setter.invoke(builder, value);
                    }
                  } catch (NoSuchMethodException e) {
                    log.debug("No setter for: " + fieldName);
                  } catch (Exception e) {
                    e.printStackTrace();
                  }
                }
              });

      return builder.build();
    }

    String hash() {
      return String.join(
          "|",
          taxonID,
          taxonConceptID,
          scientificNameID,
          scientificName,
          scientificNameAuthorship,
          taxonRank,
          verbatimTaxonRank,
          genericName,
          specificEpithet,
          infraspecificEpithet,
          kingdom,
          phylum,
          clazz,
          order,
          superfamily,
          family,
          subfamily,
          tribe,
          subtribe,
          genus,
          subgenus,
          species,
          nomenclaturalCode);
    }

    public Map<String, String> toCoreTermsMap() {
      Map<String, String> coreTerms = new HashMap<>();

      BiConsumer<Term, String> ifNotNull =
          (term, value) -> {
            if (value != null) {
              coreTerms.put(term.qualifiedName(), value);
            }
          };

      ifNotNull.accept(DwcTerm.taxonID, taxonID);
      ifNotNull.accept(DwcTerm.taxonConceptID, taxonConceptID);
      ifNotNull.accept(DwcTerm.scientificNameID, scientificNameID);
      ifNotNull.accept(DwcTerm.scientificName, scientificName);
      ifNotNull.accept(DwcTerm.scientificNameAuthorship, scientificNameAuthorship);
      ifNotNull.accept(DwcTerm.taxonRank, taxonRank);
      ifNotNull.accept(DwcTerm.verbatimTaxonRank, verbatimTaxonRank);
      ifNotNull.accept(DwcTerm.genericName, genericName);
      ifNotNull.accept(DwcTerm.specificEpithet, specificEpithet);
      ifNotNull.accept(DwcTerm.infraspecificEpithet, infraspecificEpithet);
      ifNotNull.accept(DwcTerm.kingdom, kingdom);
      ifNotNull.accept(DwcTerm.phylum, phylum);
      ifNotNull.accept(DwcTerm.class_, clazz);
      ifNotNull.accept(DwcTerm.order, order);
      ifNotNull.accept(DwcTerm.superfamily, superfamily);
      ifNotNull.accept(DwcTerm.family, family);
      ifNotNull.accept(DwcTerm.subfamily, subfamily);
      ifNotNull.accept(DwcTerm.tribe, tribe);
      ifNotNull.accept(DwcTerm.subtribe, subtribe);
      ifNotNull.accept(DwcTerm.genus, genus);
      ifNotNull.accept(DwcTerm.subgenus, subgenus);
      ifNotNull.accept(GbifTerm.species, species);
      ifNotNull.accept(DwcTerm.nomenclaturalCode, nomenclaturalCode);
      return coreTerms;
    }
  }
}
