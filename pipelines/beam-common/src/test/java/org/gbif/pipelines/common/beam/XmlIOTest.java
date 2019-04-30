package org.gbif.pipelines.common.beam;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class XmlIOTest {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void oneXmlFilesTest() {

    // State
    final String path = getClass().getResource("/xml/f0c01a2d-eaa7-4802-bd7f-eb6aad1a4a7a/response.00002.xml").getPath();

    final Map<String, String> coreMap1 = new HashMap<>();
    coreMap1.put(DwcTerm.basisOfRecord.qualifiedName(),"PreservedSpecimen");
    coreMap1.put(DwcTerm.eventDate.qualifiedName(),"1890");
    coreMap1.put(DwcTerm.kingdom.qualifiedName(),"Animalia");
    coreMap1.put(DwcTerm.catalogNumber.qualifiedName(),"Ob-0363");
    coreMap1.put(DwcTerm.collectionCode.qualifiedName(),"Ob");
    coreMap1.put(DwcTerm.family.qualifiedName(),"Tenebrionidae");
    coreMap1.put(DwcTerm.order.qualifiedName(),"Coleoptera");
    coreMap1.put(DwcTerm.institutionCode.qualifiedName(),"ZFMK");
    coreMap1.put(DwcTerm.scientificName.qualifiedName(),"Tenebrionidae Latreille, 1802");
    coreMap1.put(DwcTerm.recordedBy.qualifiedName(),"de Vauloger");
    coreMap1.put(DwcTerm.locality.qualifiedName(),"Algeria, Tunisia, Sweden, other countries");

    final ExtendedRecord expected = ExtendedRecord.newBuilder()
        .setId("ZFMK/Ob/223903199/909708/169734")
        .setCoreRowType(DwcTerm.Occurrence.qualifiedName())
        .setCoreTerms(coreMap1)
        .build();

    // When
    PCollection<ExtendedRecord> result = p.apply(XmlIO.read(path));

    // Should
    PAssert.that(result).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void severalXmlFilesTest() {

    // State
    final String path = getClass().getResource("/xml/f0c01a2d-eaa7-4802-bd7f-eb6aad1a4a7a") + "/*.xml";

    final Map<String, String> coreMap1 = new HashMap<>();
    coreMap1.put(DwcTerm.basisOfRecord.qualifiedName(),"PreservedSpecimen");
    coreMap1.put(DwcTerm.eventDate.qualifiedName(),"1890");
    coreMap1.put(DwcTerm.kingdom.qualifiedName(),"Animalia");
    coreMap1.put(DwcTerm.catalogNumber.qualifiedName(),"Ob-0363");
    coreMap1.put(DwcTerm.collectionCode.qualifiedName(),"Ob");
    coreMap1.put(DwcTerm.family.qualifiedName(),"Tenebrionidae");
    coreMap1.put(DwcTerm.order.qualifiedName(),"Coleoptera");
    coreMap1.put(DwcTerm.institutionCode.qualifiedName(),"ZFMK");
    coreMap1.put(DwcTerm.scientificName.qualifiedName(),"Tenebrionidae Latreille, 1802");
    coreMap1.put(DwcTerm.recordedBy.qualifiedName(),"de Vauloger");
    coreMap1.put(DwcTerm.locality.qualifiedName(),"Algeria, Tunisia, Sweden, other countries");

    final ExtendedRecord ex1 = ExtendedRecord.newBuilder()
        .setId("ZFMK/Ob/223903199/909708/169734")
        .setCoreRowType(DwcTerm.Occurrence.qualifiedName())
        .setCoreTerms(coreMap1)
        .build();

    final Map<String, String> coreMap2 = new HashMap<>();
    coreMap2.put(DwcTerm.basisOfRecord.qualifiedName(),"PreservedSpecimen");
    coreMap2.put(DwcTerm.kingdom.qualifiedName(),"Animalia");
    coreMap2.put(DwcTerm.catalogNumber.qualifiedName(),"Ob-0001");
    coreMap2.put(DwcTerm.collectionCode.qualifiedName(),"Ob");
    coreMap2.put(DwcTerm.family.qualifiedName(),"Curculionidae");
    coreMap2.put(DwcTerm.institutionCode.qualifiedName(),"ZFMK");
    coreMap2.put(DwcTerm.scientificName.qualifiedName(),"Curculionidae Latreille, 1802");

    final ExtendedRecord ex2 = ExtendedRecord.newBuilder()
        .setId("ZFMK/Ob/223902837/908708/169372")
        .setCoreRowType(DwcTerm.Occurrence.qualifiedName())
        .setCoreTerms(coreMap2)
        .build();

    final List<ExtendedRecord> expected = Arrays.asList(ex1, ex2);

    // When
    PCollection<ExtendedRecord> result = p.apply(XmlIO.read(path));

    // Should
    PAssert.that(result).containsInAnyOrder(expected);
    p.run();
  }

  @Test(expected = PipelineExecutionException.class)
  public void emptyFilesPathTest() {

    // State
    final String path = null;

    // When
    p.apply(XmlIO.read(path));

    // Should
    p.run();
  }
}