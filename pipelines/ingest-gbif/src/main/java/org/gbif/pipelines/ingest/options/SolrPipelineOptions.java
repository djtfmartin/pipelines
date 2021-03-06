package org.gbif.pipelines.ingest.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/** Main pipeline options necessary for Elasticsearch index properties */
public interface SolrPipelineOptions extends PipelineOptions, InterpretationPipelineOptions {

    @Description("SOLR collection to index into")
    @Default.String("biocache")
    String getSolrCollection();
    void setSolrCollection(String solrCollection);

    @Description("List of Zookeeper hosts.")
    String getZkHost();
    void setZkHost(String zkHosts);

    @Description("Include sampling")
    @Default.Boolean(false)
    Boolean getIncludeSampling();
    void setIncludeSampling(Boolean includeSampling);

    @Description("Include gbif taxonomy")
    @Default.Boolean(false)
    Boolean getIncludeGbifTaxonomy();
    void setIncludeGbifTaxonomy(Boolean includeGbifTaxonomy);
}