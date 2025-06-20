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
import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareValue;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.interpretation.transform.MultiTaxonomyTransform;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MultiTaxonRecord;

public class TaxonomyInterpretation {

  /**
   * Interprets the taxonomic information from the extended records
   *
   * @param config the configuration containing API URLs and parallelism settings
   * @param spark the Spark session to use for processing
   * @param source the dataset of extended records to process
   */
  public static Dataset<MultiTaxonRecord> taxonomyTransform(
      Config config, SparkSession spark, Dataset<ExtendedRecord> source) {

    MultiTaxonomyTransform multiTaxonomyTransform =
        MultiTaxonomyTransform.builder()
            .nameUsageMatchApiUrl(config.getSpeciesMatchAPI())
            .checklistKeys(config.getChecklistKeys())
            .build();

    // extract the taxonomy from the extended records
    Dataset<RecordWithTaxonomy> recordWithTaxonomy =
        source.map(
            (MapFunction<ExtendedRecord, RecordWithTaxonomy>)
                er -> {
                  Taxonomy taxonomy = Taxonomy.buildFrom(er);
                  return RecordWithTaxonomy.builder()
                      .id(er.getId())
                      .taxonomyHash(taxonomy.hash())
                      .taxonomy(taxonomy)
                      .build();
                },
            Encoders.bean(RecordWithTaxonomy.class));
    recordWithTaxonomy.createOrReplaceTempView("record_with_taxonomy");

    // distinct the classifications to lookup
    Dataset<TaxonomyInterpretation.Taxonomy> distinctClassifications =
        spark
            .sql("SELECT DISTINCT taxonomy.* FROM record_with_taxonomy")
            .repartition(config.getSpeciesMatchParallelism())
            .as(Encoders.bean(TaxonomyInterpretation.Taxonomy.class));

    // lookup the distinct classifications, and create a dictionary of the results
    Dataset<KeyedMultiTaxonRecord> keyedTaxonomy =
        distinctClassifications.map(
            (MapFunction<TaxonomyInterpretation.Taxonomy, KeyedMultiTaxonRecord>)
                taxonomy -> {

                  // HACK - the function takes ExtendedRecord, but we have a Location
                  ExtendedRecord er =
                      ExtendedRecord.newBuilder()
                          .setId("UNUSED_BUT_NECESSARY")
                          .setCoreTerms(taxonomy.toCoreTermsMap())
                          .build();

                  // look them up
                  Optional<MultiTaxonRecord> converted = multiTaxonomyTransform.convert(er);
                  if (converted.isPresent()) {
                    return KeyedMultiTaxonRecord.builder()
                        .key(taxonomy.hash())
                        .multiTaxonRecord(converted.get())
                        .build();
                  } else {
                    return KeyedMultiTaxonRecord.builder()
                        .key(taxonomy.hash())
                        .build(); // TODO: null handling?
                  }
                },
            Encoders.bean(KeyedMultiTaxonRecord.class));
    keyedTaxonomy.createOrReplaceTempView("key_taxonomy");

    // join the dictionary back to the source records
    Dataset<RecordWithMultiTaxonRecord> expanded =
        spark
            .sql(
                "SELECT id, multiTaxonRecord "
                    + "FROM record_with_taxonomy r "
                    + "LEFT JOIN key_taxonomy l ON r.taxonomyHash = l.key")
            .as(Encoders.bean(RecordWithMultiTaxonRecord.class));

    return expanded.map(
        (MapFunction<RecordWithMultiTaxonRecord, MultiTaxonRecord>)
            r -> {
              MultiTaxonRecord multiTaxonRecord =
                  r.getMultiTaxonRecord() == null
                      ? MultiTaxonRecord.newBuilder().build()
                      : r.getMultiTaxonRecord();

              multiTaxonRecord.setId(r.getId());
              return multiTaxonRecord;
            },
        Encoders.bean(MultiTaxonRecord.class));
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class RecordWithTaxonomy {
    private String id;
    private String taxonomyHash;
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
  public static class RecordWithMultiTaxonRecord {
    private String id;
    private MultiTaxonRecord multiTaxonRecord;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Taxonomy {
    private String id;
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

    static Taxonomy buildFrom(ExtendedRecord er) {
      return Taxonomy.builder()
          .id(er.getId())
          .taxonID(extractNullAwareValue(er, DwcTerm.taxonID))
          .taxonConceptID(extractNullAwareValue(er, DwcTerm.taxonConceptID))
          .scientificNameID(extractNullAwareValue(er, DwcTerm.scientificNameID))
          .scientificName(extractNullAwareValue(er, DwcTerm.scientificName))
          .scientificNameAuthorship(extractNullAwareValue(er, DwcTerm.scientificNameAuthorship))
          .taxonRank(extractNullAwareValue(er, DwcTerm.taxonRank))
          .verbatimTaxonRank(extractNullAwareValue(er, DwcTerm.verbatimTaxonRank))
          .genericName(extractNullAwareValue(er, DwcTerm.genericName))
          .specificEpithet(extractNullAwareValue(er, DwcTerm.specificEpithet))
          .infraspecificEpithet(extractNullAwareValue(er, DwcTerm.infraspecificEpithet))
          .kingdom(extractNullAwareValue(er, DwcTerm.kingdom))
          .phylum(extractNullAwareValue(er, DwcTerm.phylum))
          .clazz(extractNullAwareValue(er, DwcTerm.class_))
          .order(extractNullAwareValue(er, DwcTerm.order))
          .superfamily(extractNullAwareValue(er, DwcTerm.superfamily))
          .family(extractNullAwareValue(er, DwcTerm.family))
          .subfamily(extractNullAwareValue(er, DwcTerm.subfamily))
          .tribe(extractNullAwareValue(er, DwcTerm.tribe))
          .subtribe(extractNullAwareValue(er, DwcTerm.subtribe))
          .genus(extractNullAwareValue(er, DwcTerm.genus))
          .subgenus(extractNullAwareValue(er, DwcTerm.subgenus))
          .species(extractNullAwareValue(er, GbifTerm.species))
          .build();
    }

    String hash() {
      return String.join(
          "|",
          id,
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
          species);
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
      return coreTerms;
    }
  }
}
