package org.gbif.pipelines.core.interpreters.core;

import java.util.ArrayList;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.core.interpreters.model.*;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;
import org.gbif.pipelines.core.utils.VocabularyConceptFactory;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class VocabularyInterpreter {

  /** Values taken from <a href="https://github.com/gbif/vocabulary/issues/87">here</a> */
  private static final Set<String> SUSPECTED_TYPE_STATUS_VALUES =
      Set.of("?", "possible", "possibly", "potential", "maybe", "perhaps");

  /** {@link DwcTerm#lifeStage} interpretation. */
  public static BiConsumer<ExtendedRecord, BasicRecord> interpretLifeStage(
      VocabularyService vocabularyService,
      Supplier<VocabularyConcept> supplier,
      Supplier<VocabularyTag> supplierTag) {
    return (er, br) ->
        interpretVocabulary(er, DwcTerm.lifeStage, vocabularyService, supplier, supplierTag)
            .ifPresent(br::setLifeStage);
  }

  /** {@link DwcTerm#establishmentMeans} interpretation. */
  public static BiConsumer<ExtendedRecord, BasicRecord> interpretEstablishmentMeans(
      VocabularyService vocabularyService,
      Supplier<VocabularyConcept> supplier,
      Supplier<VocabularyTag> supplierTag) {
    return (er, br) ->
        interpretVocabulary(
                er, DwcTerm.establishmentMeans, vocabularyService, supplier, supplierTag)
            .ifPresent(br::setEstablishmentMeans);
  }

  /** {@link DwcTerm#degreeOfEstablishment} interpretation. */
  public static BiConsumer<ExtendedRecord, BasicRecord> interpretDegreeOfEstablishment(
      VocabularyService vocabularyService,
      Supplier<VocabularyConcept> supplier,
      Supplier<VocabularyTag> supplierTag) {
    return (er, br) ->
        interpretVocabulary(
                er, DwcTerm.degreeOfEstablishment, vocabularyService, supplier, supplierTag)
            .ifPresent(br::setDegreeOfEstablishment);
  }

  /** {@link DwcTerm#pathway} interpretation. */
  public static BiConsumer<ExtendedRecord, BasicRecord> interpretPathway(
      VocabularyService vocabularyService,
      Supplier<VocabularyConcept> supplier,
      Supplier<VocabularyTag> supplierTag) {
    return (er, br) ->
        interpretVocabulary(er, DwcTerm.pathway, vocabularyService, supplier, supplierTag)
            .ifPresent(br::setPathway);
  }

  /** {@link DwcTerm#pathway} interpretation. */
  public static BiConsumer<ExtendedRecord, EventCoreRecord> interpretEventType(
      VocabularyService vocabularyService,
      Supplier<VocabularyConcept> supplier,
      Supplier<VocabularyTag> supplierTag) {
    return (er, ecr) ->
        interpretVocabulary(er, DwcTerm.eventType, vocabularyService, supplier, supplierTag)
            .ifPresent(ecr::setEventType);
  }

  /** {@link DwcTerm#typeStatus} interpretation. */
  public static BiConsumer<ExtendedRecord, BasicRecord> interpretTypeStatus(
      VocabularyService vocabularyService,
      Supplier<VocabularyConcept> supplier,
      Supplier<VocabularyTag> supplierTag) {
    return (er, br) ->
        er.extractListValue(DwcTerm.typeStatus)
            .forEach(
                value ->
                    interpretVocabulary(
                            DwcTerm.typeStatus,
                            value,
                            vocabularyService,
                            v -> {
                              if (SUSPECTED_TYPE_STATUS_VALUES.stream()
                                  .anyMatch(sts -> v.toLowerCase().contains(sts))) {
                                br.addIssue(OccurrenceIssue.SUSPECTED_TYPE);
                              } else {
                                br.addIssue(OccurrenceIssue.TYPE_STATUS_INVALID);
                              }
                            },
                            supplier,
                            supplierTag)
                        .ifPresent(
                            v -> {
                              if (br.getTypeStatus() == null) {
                                br.setTypeStatus(new ArrayList<>());
                              }
                              br.getTypeStatus().add(v);
                            }));
  }

  /** {@link DwcTerm#sex} interpretation. */
  public static BiConsumer<ExtendedRecord, BasicRecord> interpretSex(
      VocabularyService vocabularyService,
      Supplier<VocabularyConcept> supplier,
      Supplier<VocabularyTag> supplierTag) {
    return (er, br) ->
        interpretVocabulary(er, DwcTerm.sex, vocabularyService, supplier, supplierTag)
            .ifPresent(br::setSex);
  }

  private static Optional<VocabularyConcept> interpretVocabulary(
      ExtendedRecord er,
      Term term,
      VocabularyService vocabularyService,
      Supplier<VocabularyConcept> supplier,
      Supplier<VocabularyTag> supplierTag) {
    return interpretVocabulary(
        term, er.extractNullAwareValue(term), vocabularyService, null, supplier, supplierTag);
  }

  static Optional<VocabularyConcept> interpretVocabulary(
      Term term,
      String value,
      VocabularyService vocabularyService,
      Supplier<VocabularyConcept> supplier,
      Supplier<VocabularyTag> supplierTag) {
    return interpretVocabulary(term, value, vocabularyService, null, supplier, supplierTag);
  }

  private static Optional<VocabularyConcept> interpretVocabulary(
      ExtendedRecord er,
      Term term,
      VocabularyService vocabularyService,
      Consumer<String> issueFn,
      Supplier<VocabularyConcept> supplier,
      Supplier<VocabularyTag> supplierTag) {
    return interpretVocabulary(
        term, er.extractNullAwareValue(term), vocabularyService, issueFn, supplier, supplierTag);
  }

  static Optional<VocabularyConcept> interpretVocabulary(
      Term term,
      String value,
      VocabularyService vocabularyService,
      Consumer<String> issueFn,
      Supplier<VocabularyConcept> supplier,
      Supplier<VocabularyTag> supplierTag) {

    if (vocabularyService == null) {
      return Optional.empty();
    }

    if (value != null) {
      Optional<VocabularyConcept> result =
          vocabularyService
              .get(term)
              .flatMap(lookup -> Optional.of(value).flatMap(lookup::lookup))
              .map(t -> VocabularyConceptFactory.createConcept(t, supplier, supplierTag));
      if (result.isEmpty() && issueFn != null) {
        issueFn.accept(value);
      }
      return result;
    }

    return Optional.empty();
  }
}
