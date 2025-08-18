/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.pipelines.interpretation.spark;

import static org.gbif.pipelines.core.interpreters.metadata.MetadataInterpreter.*;
import static org.gbif.pipelines.interpretation.spark.GrscicollInterpretation.grscicollTransform;
import static org.gbif.pipelines.interpretation.spark.HdfsView.transformToHdfsView;
import static org.gbif.pipelines.interpretation.spark.JsonView.transformToJsonView;
import static org.gbif.pipelines.interpretation.spark.LocationInterpretation.locationTransform;
import static org.gbif.pipelines.interpretation.spark.TaxonomyInterpretation.taxonomyTransform;
import static org.gbif.pipelines.interpretation.spark.TemporalInterpretation.temporalTransform;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import okhttp3.OkHttpClient;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.gbif.api.model.registry.Endpoint;
import org.gbif.api.vocabulary.License;
import org.gbif.common.parsers.LicenseParser;
import org.gbif.pipelines.core.ws.metadata.MetadataService;
import org.gbif.pipelines.core.ws.metadata.response.Installation;
import org.gbif.pipelines.core.ws.metadata.response.Network;
import org.gbif.pipelines.core.ws.metadata.response.Organization;
import org.gbif.pipelines.interpretation.transform.BasicTransform;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

public class Interpretation implements Serializable {
  public static void main(String[] args) throws IOException {

    if (args.length < 2) {
      System.err.println(
          "Usage: java -jar ingest-gbif-spark-<version>.jar <config.yaml> <datasetID>");
      System.exit(1);
    }

    Config config = Config.fromFirstArg(args);
    String datasetID = args[1];

    //    SparkSession.Builder sb = SparkSession.builder();

    //    if (config.getSparkRemote() != null) {
    //      sb.remote(config.getSparkRemote());
    //    }

    //    SparkSession spark = sb.getOrCreate();

    SparkSession spark =
        SparkSession.builder()
            .appName("Run local spark")
            .master("local[*]") // Use local mode with all cores
            .getOrCreate();

    //    if (config.getJarPath() != null) {
    //      spark.addArtifact(config.getJarPath());
    //    }

    // Read the verbatim input
    Dataset<ExtendedRecord> records =
        spark.read().format("avro").load(config.getInput()).as(Encoders.bean(ExtendedRecord.class));

    MetadataService metadataService = createMetadataService(config.getMetadataAPI());
    MetadataRecord metadata = getMetadataRecord(metadataService, datasetID);

    // Run the interpretations
    Dataset<BasicRecord> basic = basicTransform(config, records);
    Dataset<LocationRecord> location = locationTransform(config, spark, records);
    Dataset<TemporalRecord> temporal = temporalTransform(records);
    Dataset<MultiTaxonRecord> taxonomy = taxonomyTransform(config, spark, records);
    Dataset<GrscicollRecord> grscicoll = grscicollTransform(config, spark, records, metadata);

    //    Dataset<VerbatimRecord> verbatim = verbatimTransform(records);
    //    Dataset<AudubonRecord> audubon = audubonTransform(config, spark, records);
    //    Dataset<ImageRecord> image = imageTransform(config, spark, records);
    //    Dataset<DnaDerivedDataRecord> image = dnaDerivedDataTransform(config, spark, records);
    //    Dataset<MultimediaRecord> image = multimediaTransform(config, spark, records);

    // import org.gbif.pipelines.transforms.specific.ClusteringTransform;
    // import org.gbif.pipelines.transforms.specific.GbifIdAbsentTransform;
    // import org.gbif.pipelines.transforms.specific.GbifIdTransform;

    // Write the intermediate output (useful for debugging)
    basic.write().mode("overwrite").parquet(config.getOutput() + "/basic");
    location.write().mode("overwrite").parquet(config.getOutput() + "/location");
    temporal.write().mode("overwrite").parquet(config.getOutput() + "/temporal");
    taxonomy.write().mode("overwrite").parquet(config.getOutput() + "/taxonomy");
    grscicoll.write().mode("overwrite").parquet(config.getOutput() + "/grscicoll");

    // hdfs
    Dataset<OccurrenceHdfsRecord> hdfsView =
        transformToHdfsView(metadata, basic, location, taxonomy, temporal, grscicoll);

    DataFrameWriter<OccurrenceHdfsRecord> writer = hdfsView.write().mode("overwrite");
    writer.parquet(config.getOutput() + "/hdfsview");

    // json  - for elastic indexing
    Dataset<OccurrenceJsonRecord> jsonView =
        transformToJsonView(metadata, basic, location, taxonomy, temporal, grscicoll);

    DataFrameWriter<OccurrenceJsonRecord> jsonWriter = jsonView.write().mode("overwrite");
    jsonWriter.parquet(config.getOutput() + "/json");

    spark.close();
  }

  private static MetadataRecord getMetadataRecord(MetadataService client, String datasetKey)
      throws IOException {
    Response<org.gbif.pipelines.core.ws.metadata.response.Dataset> resp =
        client.getDataset(datasetKey).execute();

    if (!resp.isSuccessful() || resp.body() == null) {
      throw new IOException("Failed to fetch metadata for dataset: " + datasetKey);
    }
    org.gbif.pipelines.core.ws.metadata.response.Dataset dataset = resp.body();

    MetadataRecord mdr = new MetadataRecord();

    // https://github.com/gbif/pipelines/issues/401
    License license = getLicense(dataset.getLicense());
    if (license == null || license == License.UNSPECIFIED || license == License.UNSUPPORTED) {
      throw new IllegalArgumentException("Dataset licence can't be UNSPECIFIED or UNSUPPORTED!");
    } else {
      mdr.setLicense(license.name());
    }

    mdr.setDatasetTitle(dataset.getTitle());
    mdr.setInstallationKey(dataset.getInstallationKey());
    mdr.setPublishingOrganizationKey(dataset.getPublishingOrganizationKey());

    List<Endpoint> endpoints = prioritySortEndpoints(dataset.getEndpoints());
    if (!endpoints.isEmpty()) {
      mdr.setProtocol(endpoints.get(0).getType().name());
    }

    List<Network> networkList = client.getNetworks(datasetKey).execute().body();
    if (networkList != null && !networkList.isEmpty()) {
      mdr.setNetworkKeys(
          networkList.stream()
              .map(Network::getKey)
              .filter(Objects::nonNull)
              .collect(Collectors.toList()));
    }

    Organization organization =
        client.getOrganization(dataset.getPublishingOrganizationKey()).execute().body();
    mdr.setEndorsingNodeKey(organization.getEndorsingNodeKey());
    mdr.setPublisherTitle(organization.getTitle());
    mdr.setDatasetPublishingCountry(organization.getCountry());

    getLastCrawledDate(dataset.getMachineTags()).ifPresent(d -> mdr.setLastCrawled(d.getTime()));

    if (Objects.nonNull(dataset.getProject())) {
      mdr.setProjectId(dataset.getProject().getIdentifier());
      if (Objects.nonNull(dataset.getProject().getProgramme())) {
        mdr.setProgrammeAcronym(dataset.getProject().getProgramme().getAcronym());
      }
    }

    Installation installation =
        client.getInstallation(dataset.getInstallationKey()).execute().body();
    mdr.setHostingOrganizationKey(installation.getOrganizationKey());

    copyMachineTags(dataset.getMachineTags(), mdr);
    return mdr;
  }

  /** Returns ENUM instead of url string */
  private static License getLicense(String url) {
    URI uri =
        Optional.ofNullable(url)
            .map(
                x -> {
                  try {
                    return URI.create(x);
                  } catch (IllegalArgumentException ex) {
                    return null;
                  }
                })
            .orElse(null);
    License license = LicenseParser.getInstance().parseUriThenTitle(uri, null);
    // UNSPECIFIED must be mapped to null
    return License.UNSPECIFIED == license ? null : license;
  }

  private static Dataset<BasicRecord> basicTransform(
      Config config, Dataset<ExtendedRecord> source) {
    return source.map(
        (MapFunction<ExtendedRecord, BasicRecord>)
            er ->
                BasicTransform.builder()
                    .useDynamicPropertiesInterpretation(true)
                    .vocabularyApiUrl(config.getVocabularyApiUrl())
                    .build()
                    .convert(er)
                    .get(),
        Encoders.bean(BasicRecord.class));
  }

  public static MetadataService createMetadataService(String apiUrl) {

    // create client
    OkHttpClient client =
        new OkHttpClient.Builder()
            .connectTimeout(60, TimeUnit.SECONDS)
            .readTimeout(60, TimeUnit.SECONDS)
            .build();

    // create service
    Retrofit retrofit =
        new Retrofit.Builder()
            .client(client)
            .baseUrl(apiUrl)
            .addConverterFactory(JacksonConverterFactory.create())
            .validateEagerly(true)
            .build();

    return retrofit.create(MetadataService.class);
  }
}
