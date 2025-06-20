package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.api.model.collections.lookup.Match.Reason.DIFFERENT_OWNER;
import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;
import static org.gbif.pipelines.core.utils.ModelUtils.checkNullOrEmpty;
import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareValue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.util.function.BiConsumer;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.collections.lookup.Match.MatchType;
import org.gbif.api.model.collections.lookup.Match.Status;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.grscicoll.GrscicollLookupRequest;
import org.gbif.pipelines.core.converters.GrscicollRecordConverter;
import org.gbif.pipelines.core.interpreters.model.ExtendedRecord;
import org.gbif.pipelines.core.interpreters.model.GrscicollRecord;
import org.gbif.pipelines.core.interpreters.model.MetadataRecord;
import org.gbif.pipelines.core.parsers.VocabularyParser;
import org.gbif.rest.client.grscicoll.GrscicollLookupResponse;
import org.gbif.rest.client.grscicoll.GrscicollLookupResponse.Match;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GrscicollInterpreter {

  public static BiConsumer<ExtendedRecord, GrscicollRecord> grscicollInterpreter(
      KeyValueStore<GrscicollLookupRequest, GrscicollLookupResponse> kvStore, MetadataRecord mdr) {
    return (er, gr) -> {
      if (kvStore == null || mdr == null) {
        return;
      }

      if (er == null){
        throw new IllegalArgumentException("Grscicoll interpreter can't be null");
      }
      er.checkEmpty();

      if (!isSpecimenRecord(er)) {
        log.debug(
            "Skipped GrSciColl Lookup for record {} because it's not an specimen record",
            er.getId());
        return;
      }

      GrscicollLookupRequest lookupRequest =
          GrscicollLookupRequest.builder()
              .withInstitutionId(er.extractNullAwareValue(DwcTerm.institutionID))
              .withInstitutionCode(er.extractNullAwareValue(DwcTerm.institutionCode))
              .withOwnerInstitutionCode(er.extractNullAwareValue(DwcTerm.ownerInstitutionCode))
              .withCollectionId(er.extractNullAwareValue(DwcTerm.collectionID))
              .withCollectionCode(er.extractNullAwareValue(DwcTerm.collectionCode))
              .withDatasetKey(mdr.getDatasetKey())
              .withCountry(mdr.getDatasetPublishingCountry())
              .build();

      if (isEmptyRequest(lookupRequest)) {
        // skip the call
        log.debug(
            "Skipped GrSciColl Lookup for record {} due to missing collections fields", er.getId());
        return;
      }

      GrscicollLookupResponse lookupResponse = null;

      try {
        lookupResponse = kvStore.get(lookupRequest);
      } catch (Exception ex) {
        log.error("Error calling the GrSciColl lookup ws", ex);
      }

      if (isEmptyResponse(lookupResponse)) {
        // this shouldn't happen but we check it not to flag an issue in these cases
        log.warn("Empty GrSciColl lookup response for record {}", er.getId());
        return;
      }

      gr.setId(er.getId());

      // institution match
      Match institutionMatchResponse = lookupResponse.getInstitutionMatch();
      if (institutionMatchResponse.getMatchType() == MatchType.NONE) {
        gr.addIssue(getInstitutionMatchNoneIssue(institutionMatchResponse.getStatus()));

        // we skip the collections when there is no institution match
        return;
      }

      gr.setInstitutionMatch(GrscicollRecordConverter.convertMatch(institutionMatchResponse));

      if (institutionMatchResponse.getMatchType() == MatchType.FUZZY) {
        gr.addIssue(OccurrenceIssue.INSTITUTION_MATCH_FUZZY);
      }

      // https://github.com/gbif/registry/issues/496 we accept matches that have different owner,
      // but we flag them
      if (institutionMatchResponse.getReasons() != null
          && institutionMatchResponse.getReasons().contains(DIFFERENT_OWNER)) {
        gr.addIssue(OccurrenceIssue.DIFFERENT_OWNER_INSTITUTION);
      }

      // collection match
      Match collectionMatchResponse = lookupResponse.getCollectionMatch();
      if (collectionMatchResponse.getMatchType() == MatchType.NONE) {
        gr.addIssue(getCollectionMatchNoneIssue(collectionMatchResponse.getStatus()));
      } else {
        gr.setCollectionMatch(GrscicollRecordConverter.convertMatch(collectionMatchResponse));

        if (collectionMatchResponse.getMatchType() == MatchType.FUZZY) {
          gr.addIssue(OccurrenceIssue.COLLECTION_MATCH_FUZZY);
        }
      }
    };
  }

  private static boolean isSpecimenRecord(ExtendedRecord er) {

    Function<ParseResult<BasisOfRecord>, BasisOfRecord> fn =
        parseResult -> {
          if (parseResult.isSuccessful()) {
            return parseResult.getPayload();
          } else {
            return BasisOfRecord.OCCURRENCE;
          }
        };

    BasisOfRecord bor =
        VocabularyParser.basisOfRecordParser().map(er, fn).orElse(BasisOfRecord.OCCURRENCE);

    return bor == BasisOfRecord.PRESERVED_SPECIMEN
        || bor == BasisOfRecord.FOSSIL_SPECIMEN
        || bor == BasisOfRecord.LIVING_SPECIMEN
        || bor == BasisOfRecord.MATERIAL_SAMPLE
        || bor == BasisOfRecord.MATERIAL_CITATION;
  }

  @VisibleForTesting
  static OccurrenceIssue getInstitutionMatchNoneIssue(Status status) {
    if (status == Status.AMBIGUOUS || status == Status.AMBIGUOUS_EXPLICIT_MAPPINGS) {
      return OccurrenceIssue.AMBIGUOUS_INSTITUTION;
    }
    if (status == Status.AMBIGUOUS_OWNER) {
      return OccurrenceIssue.DIFFERENT_OWNER_INSTITUTION;
    }

    return OccurrenceIssue.INSTITUTION_MATCH_NONE;
  }

  @VisibleForTesting
  static OccurrenceIssue getCollectionMatchNoneIssue(Status status) {
    if (status == Status.AMBIGUOUS || status == Status.AMBIGUOUS_EXPLICIT_MAPPINGS) {
      return OccurrenceIssue.AMBIGUOUS_COLLECTION;
    }
    if (status == Status.AMBIGUOUS_INSTITUTION_MISMATCH) {
      return OccurrenceIssue.INSTITUTION_COLLECTION_MISMATCH;
    }

    return OccurrenceIssue.COLLECTION_MATCH_NONE;
  }

  private static boolean isEmptyRequest(GrscicollLookupRequest request) {
    return Strings.isNullOrEmpty(request.getInstitutionId())
        && Strings.isNullOrEmpty(request.getInstitutionCode())
        && Strings.isNullOrEmpty(request.getOwnerInstitutionCode())
        && Strings.isNullOrEmpty(request.getCollectionId())
        && Strings.isNullOrEmpty(request.getCollectionCode());
  }

  private static boolean isEmptyResponse(GrscicollLookupResponse response) {
    return response == null
        || (response.getInstitutionMatch() == null && response.getCollectionMatch() == null);
  }
}
