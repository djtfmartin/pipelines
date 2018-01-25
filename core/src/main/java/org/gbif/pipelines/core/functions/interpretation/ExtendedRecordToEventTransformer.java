package org.gbif.pipelines.core.functions.interpretation;

import org.gbif.common.parsers.NumberParser;
import org.gbif.dwca.avro.Event;
import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This function converts an extended record to an interpreted KeyValue of occurenceId and Event.
 * This function returns multiple outputs,
 * a. Interpreted version of raw temporal data as KV<String,Event>
 * b. Issues and lineages applied on raw data to get the interpreted result, as KV<String,IssueLineageRecord>
 *
 */
public class ExtendedRecordToEventTransformer extends DoFn<ExtendedRecord, KV<String,Event>> {

  private static final Logger LOG = LoggerFactory.getLogger(ExtendedRecordToEventTransformer.class);
  /**
   * tags for locating different types of outputs send by this function
   */
  public static final TupleTag<KV<String,Event>> eventDataTag = new TupleTag<KV<String,Event>>();
  public static final TupleTag<KV<String,IssueLineageRecord>> eventIssueTag = new TupleTag<KV<String,IssueLineageRecord>>();


  @ProcessElement
  public void processElement(ProcessContext ctx){
    ExtendedRecord record = ctx.element();
    Event evt = new Event();

    Map<CharSequence,List<Issue>> fieldIssueMap = new HashMap<CharSequence, List<Issue>>();
    Map<CharSequence,List<Lineage>> fieldLineageMap = new HashMap<CharSequence, List<Lineage>>();
    //mapping raw record with interpreted ones
    evt.setOccurrenceID(record.getId());
    evt.setBasisOfRecord(record.getCoreTerms().get(DwCATermIdentifier.basisOfRecord.getIdentifier()));
    evt.setEventID(record.getCoreTerms().get(DwCATermIdentifier.eventID.getIdentifier()));
    evt.setParentEventID(record.getCoreTerms().get(DwCATermIdentifier.parentEventID.getIdentifier()));
    evt.setFieldNumber(record.getCoreTerms().get(DwCATermIdentifier.fieldNumber.getIdentifier()));
    evt.setEventDate(record.getCoreTerms().get(DwCATermIdentifier.eventDate.getIdentifier()));
    evt.setStartDayOfYear(record.getCoreTerms().get(DwCATermIdentifier.startDayOfYear.getIdentifier()));
    evt.setEndDayOfYear(record.getCoreTerms().get(DwCATermIdentifier.endDayOfYear.getIdentifier()));

    /**
     * Day month year interpretation
     */
    CharSequence raw_year= record.getCoreTerms().get(DwCATermIdentifier.year.getIdentifier());
    CharSequence raw_month= record.getCoreTerms().get(DwCATermIdentifier.month.getIdentifier());
    CharSequence raw_day= record.getCoreTerms().get(DwCATermIdentifier.day.getIdentifier());

    Integer interpretedDay = null,interpretedMonth=null,interpretedYear=null;
    if(raw_day!=null){
      List<Issue> issues= new ArrayList<>();
      List<Lineage> lineages = new ArrayList<>();
      interpretedDay = NumberParser.parseInteger(raw_day.toString());
      if(interpretedDay>31 || interpretedDay<1){
        interpretedDay = null;
        issues.add(Issue.newBuilder()
                     .setOccurenceId(record.getId())
                     .setFieldName(DwCATermIdentifier.day.name())
                     .setRemark("Day out of range expected value between 1-31")
                     .build());
        lineages.add(Lineage.newBuilder().setOccurenceId(record.getId()).setFieldName(DwCATermIdentifier.day.name()).setRemark("Since day out of range interpreting as null").build());

      }
      fieldIssueMap.put(DwCATermIdentifier.day.name(),issues);
      fieldLineageMap.put(DwCATermIdentifier.day.name(),lineages);
    }

    if(raw_month!=null){
      List<Issue> issues= new ArrayList<>();
      List<Lineage> lineages = new ArrayList<>();
      interpretedMonth = NumberParser.parseInteger(raw_month.toString());
      if(interpretedMonth>12 || interpretedMonth<1) {
        interpretedMonth = null;
        issues.add(Issue.newBuilder()
                     .setOccurenceId(record.getId())
                     .setFieldName(DwCATermIdentifier.day.name())
                     .setRemark("Month out of range expected value between 1-12, inclusive")
                     .build());
        lineages.add(Lineage.newBuilder().setOccurenceId(record.getId()).setFieldName(DwCATermIdentifier.day.name()).setRemark("Since month out of range interpreting as null").build());

      }
      fieldIssueMap.put(DwCATermIdentifier.month.name(),issues);
      fieldLineageMap.put(DwCATermIdentifier.month.name(),lineages);
    }

    if(raw_year!=null){
      List<Issue> issues= new ArrayList<>();
      List<Lineage> lineages = new ArrayList<>();
      interpretedYear = NumberParser.parseInteger(raw_year.toString());
      fieldIssueMap.put(DwCATermIdentifier.year.name(),issues);
      fieldLineageMap.put(DwCATermIdentifier.year.name(),lineages);
    }

    evt.setYear(interpretedYear);
    evt.setMonth(interpretedMonth);
    evt.setDay(interpretedDay);


    evt.setVerbatimEventDate(record.getCoreTerms().get(DwCATermIdentifier.verbatimEventDate.getIdentifier()));
    evt.setHabitat(record.getCoreTerms().get(DwCATermIdentifier.habitat.getIdentifier()));
    evt.setSamplingProtocol(record.getCoreTerms().get(DwCATermIdentifier.samplingProtocol.getIdentifier()));
    evt.setSamplingEffort(record.getCoreTerms().get(DwCATermIdentifier.samplingEffort.getIdentifier()));
    evt.setSampleSizeValue(record.getCoreTerms().get(DwCATermIdentifier.sampleSizeValue.getIdentifier()));
    evt.setSampleSizeUnit(record.getCoreTerms().get(DwCATermIdentifier.sampleSizeUnit.getIdentifier()));
    evt.setFieldNotes(record.getCoreTerms().get(DwCATermIdentifier.fieldNotes.getIdentifier()));
    evt.setEventRemarks(record.getCoreTerms().get(DwCATermIdentifier.eventRemarks.getIdentifier()));
    evt.setInstitutionID(record.getCoreTerms().get(DwCATermIdentifier.institutionID.getIdentifier()));
    evt.setCollectionID(record.getCoreTerms().get(DwCATermIdentifier.collectionID.getIdentifier()));
    evt.setDatasetID(record.getCoreTerms().get(DwCATermIdentifier.datasetID.getIdentifier()));
    evt.setInstitutionCode(record.getCoreTerms().get(DwCATermIdentifier.institutionCode.getIdentifier()));
    evt.setCollectionCode(record.getCoreTerms().get(DwCATermIdentifier.collectionCode.getIdentifier()));
    evt.setDatasetName(record.getCoreTerms().get(DwCATermIdentifier.datasetName.getIdentifier()));
    evt.setOwnerInstitutionCode(record.getCoreTerms().get(DwCATermIdentifier.ownerInstitutionCode.getIdentifier()));
    evt.setDynamicProperties(record.getCoreTerms().get(DwCATermIdentifier.dynamicProperties.getIdentifier()));
    evt.setInformationWithheld(record.getCoreTerms().get(DwCATermIdentifier.informationWithheld.getIdentifier()));
    evt.setDataGeneralizations(record.getCoreTerms().get(DwCATermIdentifier.dataGeneralizations.getIdentifier()));
    //all issues and lineages are dumped on this object
    final IssueLineageRecord finalRecord = IssueLineageRecord.newBuilder().setOccurenceId(record.getId()).setFieldIssuesMap(fieldIssueMap).setFieldLineageMap(fieldLineageMap).build();

    ctx.output(eventDataTag,KV.of(evt.getOccurrenceID().toString(),evt));
    ctx.output(eventIssueTag,KV.of(evt.getOccurrenceID().toString(),finalRecord));
  }

}
