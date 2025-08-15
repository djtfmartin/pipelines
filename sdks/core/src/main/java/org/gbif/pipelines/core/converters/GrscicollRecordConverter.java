package org.gbif.pipelines.core.converters;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.model.Match;
import org.gbif.rest.client.grscicoll.GrscicollLookupResponse;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GrscicollRecordConverter {

  public static Match convertMatch(
      GrscicollLookupResponse.Match matchResponse, Supplier<Match> supplier) {
    Match match = supplier.get();
    match.setMatchType(matchResponse.getMatchType().name());
    match.setStatus(convertStatus(matchResponse.getStatus()));
    match.setReasons(convertReasons(matchResponse.getReasons()));
    match.setKey(matchResponse.getEntityMatched().getKey().toString());
    return match;
  }

  private static List<String> convertReasons(Set<GrscicollLookupResponse.Reason> reasons) {
    if (reasons == null || reasons.isEmpty()) {
      return Collections.emptyList();
    }
    return reasons.stream().map(Enum::name).collect(Collectors.toList());
  }

  private static String convertStatus(GrscicollLookupResponse.Status status) {
    return status == null ? null : status.name();
  }
}
