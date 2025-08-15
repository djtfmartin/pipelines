package org.gbif.pipelines.core.converters;

import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.gbif.api.vocabulary.MediaType;
import org.gbif.pipelines.model.AudubonRecord;
import org.gbif.pipelines.model.ImageRecord;
import org.gbif.pipelines.model.Multimedia;
import org.gbif.pipelines.model.MultimediaRecord;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MultimediaConverter {

  public static MultimediaRecord merge(
      @NonNull MultimediaRecord mr,
      @NonNull ImageRecord ir,
      @NonNull AudubonRecord ar,
      Supplier<MultimediaRecord> supplier,
      Supplier<Multimedia> mSupplier) {

    MultimediaRecord record = supplier.get();
    record.setId(mr.getId());
    record.setCreated(mr.getCreated());

    boolean isMrEmpty = mr.getMultimediaItems() == null && mr.getIssues().getIssueList().isEmpty();
    boolean isIrEmpty = ir.getImageItems() == null && ir.getIssues().getIssueList().isEmpty();
    boolean isArEmpty = ar.getAudubonItems() == null && ar.getIssues().getIssueList().isEmpty();

    if (isMrEmpty && isIrEmpty && isArEmpty) {
      return record;
    }

    Set<String> issues = new HashSet<>();
    issues.addAll(mr.getIssues().getIssueList());
    issues.addAll(ir.getIssues().getIssueList());
    issues.addAll(ar.getIssues().getIssueList());

    Map<String, Multimedia> multimediaMap = new HashMap<>();
    // The orders of puts is important
    putAllAudubonRecord(multimediaMap, ar, mSupplier);
    putAllImageRecord(multimediaMap, ir, mSupplier);
    putAllMultimediaRecord(multimediaMap, mr);

    if (!multimediaMap.isEmpty()) {
      record.setMultimediaItems(new ArrayList<>(multimediaMap.values()));
    }

    if (!issues.isEmpty()) {
      record.getIssues().getIssueList().addAll(issues);
    }

    return record;
  }

  private static void putAllMultimediaRecord(Map<String, Multimedia> map, MultimediaRecord mr) {
    Optional.ofNullable(mr.getMultimediaItems())
        .filter(i -> !i.isEmpty())
        .ifPresent(
            c ->
                c.stream()
                    .filter(
                        m ->
                            !Strings.isNullOrEmpty(m.getReferences())
                                || !Strings.isNullOrEmpty(m.getIdentifier()))
                    .forEach(
                        r -> {
                          String key =
                              Optional.ofNullable(r.getIdentifier()).orElse(r.getReferences());
                          map.putIfAbsent(key, r);
                        }));
  }

  private static void putAllImageRecord(
      Map<String, Multimedia> map, ImageRecord ir, Supplier<Multimedia> supplier) {
    Optional.ofNullable(ir.getImageItems())
        .filter(i -> !i.isEmpty())
        .ifPresent(
            c ->
                c.stream()
                    .filter(
                        m ->
                            !Strings.isNullOrEmpty(m.getReferences())
                                || !Strings.isNullOrEmpty(m.getIdentifier()))
                    .forEach(
                        r -> {
                          String key =
                              Optional.ofNullable(r.getIdentifier()).orElse(r.getReferences());
                          Multimedia multimedia = supplier.get();
                          multimedia.setAudience(r.getAudience());
                          multimedia.setContributor(r.getContributor());
                          multimedia.setCreated(r.getCreated());
                          multimedia.setCreator(r.getCreator());
                          multimedia.setDatasetId(r.getDatasetId());
                          multimedia.setDescription(r.getDescription());
                          multimedia.setFormat(r.getFormat());
                          multimedia.setIdentifier(r.getIdentifier());
                          multimedia.setLicense(r.getLicense());
                          multimedia.setPublisher(r.getPublisher());
                          multimedia.setReferences(r.getReferences());
                          multimedia.setRightsHolder(r.getRightsHolder());
                          multimedia.setTitle(r.getTitle());
                          multimedia.setType(MediaType.StillImage.name());
                          map.putIfAbsent(key, multimedia);
                        }));
  }

  private static void putAllAudubonRecord(
      Map<String, Multimedia> map, AudubonRecord ar, Supplier<Multimedia> supplier) {
    Optional.ofNullable(ar.getAudubonItems())
        .filter(i -> !i.isEmpty())
        .ifPresent(
            c ->
                c.stream()
                    .filter(m -> !Strings.isNullOrEmpty(m.getAccessUri()))
                    .forEach(
                        r -> {
                          String key = r.getAccessUri();
                          String desc =
                              Strings.isNullOrEmpty(r.getDescription())
                                  ? r.getCaption()
                                  : r.getDescription();
                          String creator =
                              Strings.isNullOrEmpty(r.getCreator())
                                  ? r.getCreatorUri()
                                  : r.getCreator();
                          String identifier =
                              Strings.isNullOrEmpty(r.getAccessUri())
                                  ? r.getIdentifier()
                                  : r.getAccessUri();
                          Multimedia multimedia = supplier.get();
                          multimedia.setCreated(r.getCreateDate());
                          multimedia.setCreator(creator);
                          multimedia.setDescription(desc);
                          multimedia.setFormat(r.getFormat());
                          multimedia.setIdentifier(identifier);
                          multimedia.setLicense(r.getRights());
                          multimedia.setRightsHolder(r.getOwner());
                          multimedia.setSource(r.getSource());
                          multimedia.setTitle(r.getTitle());
                          multimedia.setType(r.getType());
                          multimedia.setSource(r.getSource());
                          multimedia.setPublisher(r.getProviderLiteral());
                          map.putIfAbsent(key, multimedia);
                        }));
  }
}
