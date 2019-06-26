package org.gbif.pipelines.ingest.hdfs.converters;

import org.gbif.api.vocabulary.*;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.MediaType;
import org.gbif.pipelines.io.avro.NamePart;
import org.gbif.pipelines.io.avro.NameType;
import org.gbif.pipelines.io.avro.Rank;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;

public class OccurrenceHdfsRecordConverterTest {

    @Test
    public void extendedRecordMapperTest() {
        Map<String,String> coreTerms = new HashMap<>();
        coreTerms.put(DwcTerm.verbatimDepth.simpleName(),"1.0");
        coreTerms.put(DwcTerm.collectionCode.simpleName(),"C1");
        coreTerms.put(DwcTerm.institutionCode.simpleName(),"I1");
        coreTerms.put(DwcTerm.catalogNumber.simpleName(), "CN1");
        coreTerms.put(DwcTerm.class_.simpleName(), "classs");
        coreTerms.put(DcTerm.format.simpleName(), "format");
        coreTerms.put(DwcTerm.order.simpleName(), "order");
        coreTerms.put(DwcTerm.group.simpleName(), "group");
        coreTerms.put(DcTerm.date.simpleName(), "26/06/2019");
        ExtendedRecord extendedRecord = ExtendedRecord.newBuilder()
                                         .setId("1")
                                         .setCoreTerms(coreTerms).build();
        OccurrenceHdfsRecord hdfsRecord = OccurrenceHdfsRecordConverter.toOccurrenceHdfsRecord(extendedRecord);
        Assert.assertEquals("1.0", hdfsRecord.getVerbatimdepth());
        Assert.assertEquals("C1", hdfsRecord.getCollectioncode());
        Assert.assertEquals("I1", hdfsRecord.getInstitutioncode());
        Assert.assertEquals("CN1", hdfsRecord.getCatalognumber());
        Assert.assertEquals("classs", hdfsRecord.getClass$());
        Assert.assertEquals("format", hdfsRecord.getFormat());
        Assert.assertEquals("order", hdfsRecord.getOrder());
        Assert.assertEquals("group", hdfsRecord.getGroup());
        Assert.assertEquals("26/06/2019", hdfsRecord.getDate());
    }

    @Test
    public void multimediaMapperTest() {
        MultimediaRecord multimediaRecord = new MultimediaRecord();
        multimediaRecord.setId("1");
        Multimedia multimedia = new Multimedia();
        multimedia.setType(MediaType.StillImage.name());
        multimedia.setLicense(License.CC_BY_4_0.name());
        multimedia.setSource("image.jpg");
        multimediaRecord.setMultimediaItems(Collections.singletonList(multimedia));
        OccurrenceHdfsRecord hdfsRecord = OccurrenceHdfsRecordConverter.toOccurrenceHdfsRecord(multimediaRecord);
        Assert.assertTrue(hdfsRecord.getMediatype().contains(MediaType.StillImage.name()));
    }

    @Test
    public void basicRecordMapperTest() {
        long now = new Date().getTime();
        BasicRecord basicRecord = new BasicRecord();
        basicRecord.setBasisOfRecord(BasisOfRecord.HUMAN_OBSERVATION.name());
        basicRecord.setSex(Sex.HERMAPHRODITE.name());
        basicRecord.setIndividualCount(99);
        basicRecord.setLifeStage(LifeStage.GAMETE.name());
        basicRecord.setTypeStatus(TypeStatus.ALLOTYPE.name());
        basicRecord.setTypifiedName("noName");
        basicRecord.setEstablishmentMeans(EstablishmentMeans.INVASIVE.name());
        basicRecord.setCreated(now);
        OccurrenceHdfsRecord hdfsRecord = OccurrenceHdfsRecordConverter.toOccurrenceHdfsRecord(basicRecord);
        Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), hdfsRecord.getBasisofrecord());
        Assert.assertEquals(Sex.HERMAPHRODITE.name(), hdfsRecord.getSex());
        Assert.assertEquals(new Integer(99), hdfsRecord.getIndividualcount());
        Assert.assertEquals(LifeStage.GAMETE.name(), hdfsRecord.getLifestage());
        Assert.assertEquals(TypeStatus.ALLOTYPE.name(), hdfsRecord.getTypestatus());
        Assert.assertEquals("noName", hdfsRecord.getTypifiedname());
        Assert.assertEquals(EstablishmentMeans.INVASIVE.name(), hdfsRecord.getEstablishmentmeans());
    }

    @Test
    public void taxonMapperTest() {
        List<RankedName> classification = new ArrayList<>();
        classification.add(RankedName.newBuilder().setKey(2).setRank(Rank.KINGDOM).setName("Archaea").build());
        classification.add(RankedName.newBuilder().setKey(79).setRank(Rank.PHYLUM).setName("Crenarchaeota").build());
        classification.add(RankedName.newBuilder().setKey(8016360).setRank(Rank.ORDER).setName("Acidilobales").build());
        classification.add(RankedName.newBuilder().setKey(292).setRank(Rank.CLASS).setName("Thermoprotei").build());
        classification.add(RankedName.newBuilder().setKey(7785).setRank(Rank.FAMILY).setName("Caldisphaeraceae").build());
        classification.add(RankedName.newBuilder().setKey(1000002).setRank(Rank.GENUS).setName("Caldisphaera").build());
        classification.add(RankedName.newBuilder().setKey(1000003).setRank(Rank.SPECIES).setName("Caldisphaera lagunensis").build());

        ParsedName parsedName = ParsedName.newBuilder().setType(NameType.SCIENTIFIC)
                                 .setAbbreviated(Boolean.FALSE)
                                 .setBasionymAuthorship(Authorship.newBuilder()
                                                         .setYear("2003")
                                                         .setAuthors(Collections.singletonList("Itoh & al."))
                                                         .setExAuthors(Collections.emptyList())
                                                         .setEmpty(Boolean.FALSE).build())
                                 .setAutonym(Boolean.FALSE)
                                 .setBinomial(Boolean.TRUE)
                                 .setGenus("Caldisphaera")
                                 .setSpecificEpithet("lagunensis")
                                 .setNotho(NamePart.SPECIFIC)
                                 .setState(State.COMPLETE)
                                 .build();

        TaxonRecord taxonRecord = new TaxonRecord();
        RankedName rankedName = RankedName.newBuilder()
                                 .setKey(2492483)
                                 .setRank(Rank.SPECIES)
                                 .setName("Caldisphaera lagunensis Itoh & al., 2003")
                                 .build();

        taxonRecord.setUsage(rankedName);
        taxonRecord.setUsage(rankedName);
        taxonRecord.setAcceptedUsage(rankedName);
        taxonRecord.setSynonym(Boolean.FALSE);
        taxonRecord.setClassification(classification);
        taxonRecord.setUsageParsedName(parsedName);
        taxonRecord.setNomenclature(Nomenclature.newBuilder().setSource("nothing").build());

        OccurrenceHdfsRecord hdfsRecord = OccurrenceHdfsRecordConverter.toOccurrenceHdfsRecord(taxonRecord);
        Assert.assertEquals("Archaea", hdfsRecord.getKingdom());
        Assert.assertEquals(new Integer(2), hdfsRecord.getKingdomkey());

        Assert.assertEquals("Crenarchaeota", hdfsRecord.getPhylum());
        Assert.assertEquals(new Integer(79), hdfsRecord.getPhylumkey());

        Assert.assertEquals("Acidilobales", hdfsRecord.getOrder());
        Assert.assertEquals(new Integer(8016360), hdfsRecord.getOrderkey());

        Assert.assertEquals("Thermoprotei", hdfsRecord.getClass$());
        Assert.assertEquals(new Integer(292), hdfsRecord.getClasskey());

        Assert.assertEquals("Caldisphaeraceae", hdfsRecord.getFamily());
        Assert.assertEquals(new Integer(7785), hdfsRecord.getFamilykey());

        Assert.assertEquals("Caldisphaera", hdfsRecord.getGenus());
        Assert.assertEquals(new Integer(1000002), hdfsRecord.getGenuskey());

        Assert.assertEquals("Caldisphaera lagunensis", hdfsRecord.getSpecies());
        Assert.assertEquals(new Integer(1000003), hdfsRecord.getSpecieskey());

        Assert.assertEquals("2492483", hdfsRecord.getAcceptednameusageid());
        Assert.assertEquals("Caldisphaera lagunensis Itoh & al., 2003", hdfsRecord.getAcceptedscientificname());
        Assert.assertEquals(new Integer(2492483), hdfsRecord.getAcceptedtaxonkey());

        Assert.assertEquals("Caldisphaera", hdfsRecord.getGenericname());
        Assert.assertEquals("lagunensis", hdfsRecord.getSpecificepithet());

    }

    @Test
    public void temporalMapperTest() {
      String rawEventDate = "2019-01-01";

      Long eventDate = LocalDate.of(2019,1,1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();

      TemporalRecord temporalRecord = TemporalRecord.newBuilder()
                                       .setId("1")
                                       .setDay(1)
                                       .setYear(2019)
                                       .setMonth(1)
                                       .setStartDayOfYear(1)
                                       .setEventDate(EventDate.newBuilder().setLte(rawEventDate).build())
                                       .setDateIdentified(rawEventDate)
                                       .setModified(rawEventDate)
                                       .build();
        OccurrenceHdfsRecord hdfsRecord = OccurrenceHdfsRecordConverter.toOccurrenceHdfsRecord(temporalRecord);
        Assert.assertEquals(new Integer(1), hdfsRecord.getDay());
        Assert.assertEquals(new Integer(1), hdfsRecord.getMonth());
        Assert.assertEquals(new Integer(2019), hdfsRecord.getYear());
        Assert.assertEquals("1", hdfsRecord.getStartdayofyear());
        Assert.assertEquals(eventDate, hdfsRecord.getEventdate());
        Assert.assertEquals(eventDate, hdfsRecord.getDateidentified());
        Assert.assertEquals(eventDate, hdfsRecord.getModified());
    }

    @Test
    public void metadataMapperTest(){
        String datasetKey = UUID.randomUUID().toString();
        String nodeKey = UUID.randomUUID().toString();
        String installationKey = UUID.randomUUID().toString();
        String organizationKey = UUID.randomUUID().toString();
        List<String> networkKey = Collections.singletonList(UUID.randomUUID().toString());

        MetadataRecord metadataRecord = MetadataRecord.newBuilder()
                                         .setId("1")
                                         .setDatasetKey(datasetKey)
                                         .setCrawlId(1)
                                         .setDatasetPublishingCountry(Country.COSTA_RICA.getIso2LetterCode())
                                         .setLicense(License.CC_BY_4_0.name())
                                         .setNetworkKeys(networkKey)
                                         .setDatasetTitle("TestDataset")
                                         .setEndorsingNodeKey(nodeKey)
                                         .setInstallationKey(installationKey)
                                         .setLastCrawled(new Date().getTime())
                                         .setProtocol(EndpointType.DWC_ARCHIVE.name())
                                         .setPublisherTitle("Pub")
                                         .setPublishingOrganizationKey(organizationKey)
                                         .build();
        OccurrenceHdfsRecord hdfsRecord = OccurrenceHdfsRecordConverter.toOccurrenceHdfsRecord(metadataRecord);
        Assert.assertEquals(datasetKey, hdfsRecord.getDatasetkey());
        Assert.assertEquals(networkKey, hdfsRecord.getNetworkkey());
        Assert.assertEquals(installationKey, hdfsRecord.getInstallationkey());
        Assert.assertEquals(organizationKey, hdfsRecord.getPublishingorgkey());
    }

    @Test
    public void locationMapperTest() {
        LocationRecord locationRecord = LocationRecord.newBuilder()
                                        .setId("1")
                                        .setCountry(Country.COSTA_RICA.name())
                                        .setCountryCode(Country.COSTA_RICA.getIso2LetterCode())
                                        .setDecimalLatitude(9.934739)
                                        .setDecimalLongitude(-84.087502)
                                        .setContinent(Continent.NORTH_AMERICA.name())
                                        .setHasCoordinate(Boolean.TRUE)
                                        .setCoordinatePrecision(0.1)
                                        .setCoordinateUncertaintyInMeters(1.0)
                                        .setDepth(5.0)
                                        .setDepthAccuracy(0.1)
                                        .setElevation(0.0)
                                        .setElevationAccuracy(0.1)
                                        .setHasGeospatialIssue(Boolean.FALSE)
                                        .setRepatriated(Boolean.TRUE)
                                        .setStateProvince("Limon")
                                        .setWaterBody("Atlantic")
                                        .setMaximumDepthInMeters(0.1)
                                        .setMinimumDepthInMeters(0.1)
                                        .setMaximumDistanceAboveSurfaceInMeters(0.1)
                                        .setMaximumElevationInMeters(0.1)
                                        .setMinimumElevationInMeters(0.1)
                                        .build();
        OccurrenceHdfsRecord hdfsRecord = OccurrenceHdfsRecordConverter.toOccurrenceHdfsRecord(locationRecord);
        Assert.assertEquals(Country.COSTA_RICA.name(), hdfsRecord.getCounty());
        Assert.assertEquals(Country.COSTA_RICA.getIso2LetterCode(), hdfsRecord.getCountrycode());
        Assert.assertEquals(new Double(9.934739), hdfsRecord.getDecimallatitude());
        Assert.assertEquals(new Double(-84.087502), hdfsRecord.getDecimallongitude());
        Assert.assertEquals(Continent.NORTH_AMERICA.name(), hdfsRecord.getContinent());
        Assert.assertEquals(Boolean.TRUE, hdfsRecord.getHascoordinate());
        Assert.assertEquals(new Double(0.1), hdfsRecord.getCoordinateprecision());
        Assert.assertEquals(new Double(1.0), hdfsRecord.getCoordinateuncertaintyinmeters());
        Assert.assertEquals(new Double(5.0), hdfsRecord.getDepth());
        Assert.assertEquals(new Double(0.1), hdfsRecord.getDepthaccuracy());
        Assert.assertEquals(new Double(0.0), hdfsRecord.getElevation());
        Assert.assertEquals(new Double(0.1), hdfsRecord.getElevationaccuracy());
        Assert.assertEquals(Boolean.FALSE, hdfsRecord.getHasgeospatialissues());
        Assert.assertEquals(Boolean.TRUE, hdfsRecord.getRepatriated());
        Assert.assertEquals("Limon", hdfsRecord.getStateprovince());
        Assert.assertEquals("Atlantic", hdfsRecord.getWaterbody());
    }

    @Test
    public void issueMappingTest() {

        String[] issues  = {OccurrenceIssue.IDENTIFIED_DATE_INVALID.name(),
                            OccurrenceIssue.MODIFIED_DATE_INVALID.name(),
                            OccurrenceIssue.RECORDED_DATE_UNLIKELY.name()};

        TemporalRecord temporalRecord = TemporalRecord.newBuilder()
                .setId("1")
                .setDay(1)
                .setYear(2019)
                .setMonth(1)
                .setStartDayOfYear(1)
                .setIssues(IssueRecord.newBuilder()
                            .setIssueList(Arrays.asList(issues))
                            .build())
                .build();

        OccurrenceHdfsRecord hdfsRecord = OccurrenceHdfsRecordConverter.toOccurrenceHdfsRecord(temporalRecord);
        Assert.assertArrayEquals(issues, hdfsRecord.getIssue().toArray(new String[issues.length]));
    }
}