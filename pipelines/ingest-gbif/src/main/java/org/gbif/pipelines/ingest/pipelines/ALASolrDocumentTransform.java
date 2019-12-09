package org.gbif.pipelines.ingest.pipelines;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import freemarker.template.SimpleDate;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.solr.common.SolrInputDocument;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.core.converters.GbifJsonConverter;
import org.gbif.pipelines.core.converters.JsonConverter;
import org.gbif.pipelines.core.converters.MultimediaConverter;
import org.gbif.pipelines.core.utils.TemporalUtils;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.transforms.converters.GbifJsonTransform;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.BiConsumer;

import static org.apache.avro.Schema.Type.UNION;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_JSON_COUNT;

@AllArgsConstructor(staticName = "create")
public class ALASolrDocumentTransform implements Serializable {

    private static final long serialVersionUID = 1279313931024806169L;
    // Core
    @NonNull
    private final TupleTag<ExtendedRecord> erTag;
    @NonNull
    private final TupleTag<BasicRecord> brTag;
    @NonNull
    private final TupleTag<TemporalRecord> trTag;
    @NonNull
    private final TupleTag<LocationRecord> lrTag;
    @NonNull
    private final TupleTag<TaxonRecord> txrTag;
    @NonNull
    private final TupleTag<ALATaxonRecord> atxrTag;
    // Extension
    @NonNull
    private final TupleTag<MultimediaRecord> mrTag;
    @NonNull
    private final TupleTag<ImageRecord> irTag;
    @NonNull
    private final TupleTag<AudubonRecord> arTag;
    @NonNull
    private final TupleTag<MeasurementOrFactRecord> mfrTag;

    @NonNull
    private final PCollectionView<MetadataRecord> metadataView;

    public ParDo.SingleOutput<KV<String, CoGbkResult>, SolrInputDocument> converter() {

        DoFn<KV<String, CoGbkResult>, SolrInputDocument> fn = new DoFn<KV<String, CoGbkResult>, SolrInputDocument>() {

            private final Counter counter = Metrics.counter(ALASolrDocumentTransform.class, AVRO_TO_JSON_COUNT);

            @ProcessElement
            public void processElement(ProcessContext c) {
                CoGbkResult v = c.element().getValue();
                String k = c.element().getKey();

                // Core
                MetadataRecord mdr = c.sideInput(metadataView);
                ExtendedRecord er = v.getOnly(erTag, ExtendedRecord.newBuilder().setId(k).build());
                BasicRecord br = v.getOnly(brTag, BasicRecord.newBuilder().setId(k).build());
                TemporalRecord tr = v.getOnly(trTag, TemporalRecord.newBuilder().setId(k).build());
                LocationRecord lr = v.getOnly(lrTag, LocationRecord.newBuilder().setId(k).build());
                TaxonRecord txr = v.getOnly(txrTag, TaxonRecord.newBuilder().setId(k).build());
                ALATaxonRecord atxr = v.getOnly(atxrTag, ALATaxonRecord.newBuilder().setId(k).build());
                // Extension
                MultimediaRecord mr = v.getOnly(mrTag, MultimediaRecord.newBuilder().setId(k).build());
                ImageRecord ir = v.getOnly(irTag, ImageRecord.newBuilder().setId(k).build());
                AudubonRecord ar = v.getOnly(arTag, AudubonRecord.newBuilder().setId(k).build());
                MeasurementOrFactRecord mfr = v.getOnly(mfrTag, MeasurementOrFactRecord.newBuilder().setId(k).build());

                MultimediaRecord mmr = MultimediaConverter.merge(mr, ir, ar);

                List<String> skipKeys = new ArrayList<String>();

                SolrInputDocument doc = new SolrInputDocument();
                doc.setField("id", er.getId());

                addToDoc(lr, doc);
                addToDoc(tr, doc);
                addToDoc(br, doc);
                addToDoc(er, doc);
                addToDoc(mdr, doc);
                addToDoc(mdr, doc);

                //add event date
                try {
                    if (tr.getEventDate() != null && tr.getEventDate().getGte() != null) {
                        doc.setField("dwc_t_eventDateSingle", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm").parse(tr.getEventDate().getGte()));
                    } else {
                        TemporalUtils.getTemporal(tr.getYear(), tr.getMonth(), tr.getDay())
                                .ifPresent(x -> doc.setField("dwc_t_eventDateSingle", x));
                    }
                } catch (ParseException e){
                    //ignore for now
                }

                //add the classification
                List<RankedName> taxonomy = txr.getClassification();
                for (RankedName entry : taxonomy) {
                    doc.setField("gbif_s_" + entry.getRank().toString().toLowerCase() + "_id", entry.getKey());
                    doc.setField("gbif_s_" + entry.getRank().toString().toLowerCase(), entry.getName());
                }

                String rank = txr.getAcceptedUsage().getRank().toString();
                doc.setField("gbif_s_rank", txr.getAcceptedUsage().getRank().toString());
                doc.setField("gbif_s_scientificName", txr.getAcceptedUsage().getName().toString());

                Map<String,String> raw = er.getCoreTerms();
                for (Map.Entry<String, String> entry : raw.entrySet()) {
                    String key = entry.getKey();
                    if(key.startsWith("http")){
                        key = key.substring(key.lastIndexOf("/") + 1);
                    }
                    doc.setField("raw_" + key, entry.getValue().toString());
                }

                if(lr.getDecimalLatitude() != null && lr.getDecimalLongitude() != null){
                    addGeo(doc, lr.getDecimalLatitude(), lr.getDecimalLongitude());
                }

                //TODO assertions

                if (atxr.getTaxonConceptID() != null){
                    List<Schema.Field> fields = atxr.getSchema().getFields();
                    for (Schema.Field field: fields){
                        field.name();
                        Object value = atxr.get(field.name());
                        if (value != null){
                            if (value instanceof Integer){
                                doc.setField("dwc_i_" + field.name(), value);
                            } else {
                                doc.setField("dwc_s_" + field.name(), value.toString());
                            }
                        }
                    }

                    //required for EYA
                    doc.setField( "names_and_lsid", atxr.getScientificName() + "|" + atxr.getTaxonConceptID() + "|" + atxr.getVernacularName() + "|" + atxr.getKingdom() + "|" + atxr.getFamily()); // is set to IGNORE in headerAttributes
                    doc.setField( "common_name_and_lsid",  atxr.getVernacularName() + "|" + atxr.getScientificName() + "|" + atxr.getTaxonConceptID() + "|" +  atxr.getVernacularName() + "|" + atxr.getKingdom() + "|" + atxr.getFamily()); // is set to IGNORE in headerAttributes

                } else {
                    System.out.println("No match for taxonomy");
                }

                doc.setField("geospatial_kosher", lr.getHasCoordinate());
                doc.setField("first_loaded_date", new Date());

                c.output(doc);

                counter.inc();
            }
        };

        return ParDo.of(fn).withSideInputs(metadataView);
    }


    void addGeo(SolrInputDocument doc, double lat, double lon){
        double test = -90d;
        double test2 = -180d;
        String latlon = "";
        //ensure that the lat longs are in the required range before
        if (lat <= 90 && lat >= test && lon <= 180 && lon >= test2) {
            latlon = lat + "," + lat;
        }

        doc.addField("lat_long", latlon); // is set to IGNORE in headerAttributes
        doc.addField("point-1", getLatLongString(lat, lon, "#")); // is set to IGNORE in headerAttributes
        doc.addField("point-0.1", getLatLongString(lat, lon, "#.#")); // is set to IGNORE in headerAttributes
        doc.addField("point-0.01", getLatLongString(lat, lon, "#.##")); // is set to IGNORE in headerAttributes
        doc.addField("point-0.02", getLatLongStringStep(lat, lon, "#.##", 0.02)); // is set to IGNORE in headerAttributes
        doc.addField("point-0.001", getLatLongString(lat, lon, "#.###")); // is set to IGNORE in headerAttributes
        doc.addField("point-0.0001", getLatLongString(lat, lon, "#.####")); // is set to IGNORE in headerAttributes
    }

    String getLatLongStringStep(Double lat, Double lon, String format, Double step) {
        DecimalFormat df = new java.text.DecimalFormat(format);
        //By some "strange" decision the default rounding model is HALF_EVEN
        df.setRoundingMode(java.math.RoundingMode.HALF_UP);
        return df.format(Math.round(lat / step) * step) + "," + df.format(Math.round(lon / step) * step);
    }

    /**
     * Returns a lat,long string expression formatted to the supplied Double format
     */
    String getLatLongString(Double lat, Double lon, String format) {
        DecimalFormat df = new java.text.DecimalFormat(format);
        //By some "strange" decision the default rounding model is HALF_EVEN
        df.setRoundingMode(java.math.RoundingMode.HALF_UP);
        return df.format(lat) + "," + df.format(lon);
    }

    void addToDoc(SpecificRecordBase record, SolrInputDocument doc){

        Set<String> skipKeys = new HashSet<String>();
        skipKeys.add("id");
        skipKeys.add("created");
        skipKeys.add("text");
        skipKeys.add("name");
        skipKeys.add("coreRowType");
        skipKeys.add("coreTerms");
        skipKeys.add("extensions");
        skipKeys.add("usage");
        skipKeys.add("classification");

        record.getSchema().getFields().stream()
                .filter(n -> !skipKeys.contains(n.name()))
                .forEach(
                        f -> Optional.ofNullable(record.get(f.pos())).ifPresent(r -> {

                            Schema schema = f.schema();
                            Optional<Schema.Type> type = schema.getType() == UNION
                                    ? schema.getTypes().stream().filter(t -> t.getType() != Schema.Type.NULL).findFirst().map(Schema::getType)
                                    : Optional.of(schema.getType());

                            type.ifPresent(t -> {
                                switch (t) {
                                    case BOOLEAN:
                                        doc.setField("dwc_b_" + f.name(), (Boolean) r);
                                        break;
                                    case FLOAT:
                                        doc.setField("dwc_f_" + f.name(), (Float) r);
                                        break;
                                    case DOUBLE:
                                        doc.setField("dwc_d_" + f.name(), (Double) r);
                                        break;
                                    case INT:
                                        doc.setField("dwc_i_" + f.name(), (Integer) r);
                                        break;
                                    case LONG:
                                        doc.setField("dwc_l_" + f.name(), (Long) r);
                                        break;
                                    default:
                                        doc.setField("dwc_s_"+  f.name(), r.toString());
                                        break;
                                }
                            });
                        })
                );
    }
}
