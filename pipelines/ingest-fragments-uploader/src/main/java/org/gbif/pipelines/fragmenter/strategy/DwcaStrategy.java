package org.gbif.pipelines.fragmenter.strategy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.gbif.dwc.DwcFiles;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.record.StarRecord;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.fragmenter.common.StarRecordCopy;
import org.gbif.pipelines.fragmenter.record.DwcaExtensionOccurrenceRecord;
import org.gbif.pipelines.fragmenter.record.DwcaOccurrenceRecord;
import org.gbif.pipelines.fragmenter.record.OccurrenceRecord;
import org.gbif.utils.file.ClosableIterator;

import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

@NoArgsConstructor(staticName = "create")
public class DwcaStrategy implements Strategy {

  @SneakyThrows
  @Override
  public void process(Path path, Consumer<OccurrenceRecord> pushRecordFn) {
    try (ClosableIterator<StarRecord> starRecordIterator = readDwca(path)) {
      while (starRecordIterator.hasNext()) {
        StarRecord starRecord = StarRecordCopy.create(starRecordIterator.next());
        convertToRecordUnits(starRecord).forEach(pushRecordFn);
      }
    }
  }

  private ClosableIterator<StarRecord> readDwca(Path path) throws IOException {
    if (path.endsWith(".dwca")) {
      return DwcFiles.fromCompressed(path, path.getParent()).iterator();
    } else {
      return DwcFiles.fromLocation(path).iterator();
    }
  }

  private List<OccurrenceRecord> convertToRecordUnits(StarRecord starRecord) {
    List<Record> records = starRecord.extension(DwcTerm.Occurrence);
    if (records == null || records.isEmpty()) {
      return Collections.singletonList(DwcaOccurrenceRecord.create(starRecord));
    } else {
      return records.stream()
          .map(r -> DwcaExtensionOccurrenceRecord.create(starRecord.core(), r))
          .collect(Collectors.toList());
    }
  }

}
