package org.gbif.pipelines.core.interpreters.core;

import java.util.function.BiConsumer;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.interpreters.model.BasicRecord;
import org.gbif.pipelines.core.interpreters.model.ExtendedRecord;
import org.gbif.pipelines.core.interpreters.model.VocabularyConcept;
import org.gbif.pipelines.core.interpreters.model.VocabularyTag;
import org.gbif.pipelines.core.parsers.vertnet.LifeStageParser;
import org.gbif.pipelines.core.parsers.vertnet.SexParser;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;
import org.gbif.pipelines.core.utils.VocabularyConceptFactory;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DynamicPropertiesInterpreter {

  public static BiConsumer<ExtendedRecord, BasicRecord> interpretSex(
      VocabularyService vocabularyService,
      Supplier<VocabularyConcept> supplier,
      Supplier<VocabularyTag> supplierTag) {
    return (er, br) -> {
      if (br.getSex() == null) {
        vocabularyService
            .get(DwcTerm.sex)
            .flatMap(
                lookup ->
                    er.extractNullAwareOptValue(DwcTerm.dynamicProperties)
                        .flatMap(v -> SexParser.parse(v).flatMap(lookup::lookup)))
            .map(t -> VocabularyConceptFactory.createConcept(t, supplier, supplierTag))
            .ifPresent(br::setSex);
      }
    };
  }

  public static BiConsumer<ExtendedRecord, BasicRecord> interpretLifeStage(
      VocabularyService vocabularyService,
      Supplier<VocabularyConcept> supplier,
      Supplier<VocabularyTag> supplierTag) {
    return (er, br) -> {
      if (br.getLifeStage() == null) {
        vocabularyService
            .get(DwcTerm.lifeStage)
            .flatMap(
                lookup ->
                    er.extractNullAwareOptValue(DwcTerm.dynamicProperties)
                        .flatMap(v -> LifeStageParser.parse(v).flatMap(lookup::lookup)))
            .map(t -> VocabularyConceptFactory.createConcept(t, supplier, supplierTag))
            .ifPresent(br::setLifeStage);
      }
    };
  }
}
