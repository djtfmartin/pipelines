package org.gbif.pipelines.interpretation.spark;

import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;

@Slf4j
public class TableBuild {

  @Parameters(separators = "=")
  private static class Args {

    @Parameter(names = "--appName", description = "Application name", required = true)
    private String appName;

    @Parameter(names = "--datasetId", description = "Dataset ID", required = true)
    private String datasetId;

    @Parameter(names = "--attempt", description = "Attempt number", required = true)
    private int attempt;

    @Parameter(names = "--properties", description = "Path to properties file", required = true)
    private String properties;

    @Parameter(
        names = "--master",
        description = "Spark master - there for local dev only",
        required = false)
    private String master;

    @Parameter(
        names = {"--help", "-h"},
        help = true,
        description = "Show usage")
    private boolean help;
  }

  public static void main(String[] argsv) throws Exception {
    TableBuild.Args args = new TableBuild.Args();
    JCommander jCommander = new JCommander(args);
    jCommander.parse(argsv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    PipelinesConfig config = loadConfig(args.properties);
    String datasetId = args.datasetId;
    int attempt = args.attempt;

    runTableBuild(config, datasetId, attempt, args.appName, args.master);
  }

  public static void runTableBuild(
      PipelinesConfig config, String datasetId, int attempt, String appName, String master)
      throws Exception {
    String outputPath = String.format("%s/%s/%d", config.getOutputPath(), datasetId, attempt);

    SparkSession.Builder sparkBuilder = SparkSession.builder().appName(appName);
    sparkBuilder
        .enableHiveSupport()
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hive")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.warehouse.dir", "hdfs://gbif-hdfs/stackable/warehouse");

    if (master != null && !master.isEmpty()) {
      sparkBuilder = sparkBuilder.master(master);
      sparkBuilder.config("spark.driver.extraClassPath", "/etc/hadoop/conf");
      sparkBuilder.config("spark.executor.extraClassPath", "/etc/hadoop/conf");
      // FIXME
      sparkBuilder.config("spark.hadoop.hive.metastore.uris", config.getHiveMetastoreUris());
    }

    SparkSession spark = sparkBuilder.getOrCreate();
    FileSystem fs = null;
    if (master != null) {
      Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
      hadoopConf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
      hadoopConf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
      fs = FileSystem.get(hadoopConf);
    } else {
      HdfsConfigs hdfsConfigs =
          HdfsConfigs.create(config.getHdfsSiteConfig(), config.getCoreSiteConfig());
      fs = FsUtils.getFileSystem(hdfsConfigs, "/");
    }

    // load hdfs view
    Dataset<Row> hdfs = spark.read().parquet(outputPath + "/hdfs");
    String[] columns = hdfs.columns();

    StringBuilder selectBuffer = new StringBuilder();

    List<String> columnList = new ArrayList<String>();

    for (int i = 0; i < columns.length; i++) {

      if (columns[i].equalsIgnoreCase("extMultimedia")) {
        String icebergCol = "ext_multimedia";
        selectBuffer.append("`extMultimedia` AS `").append(icebergCol).append("`");
        columnList.add(icebergCol);
      } else if (columns[i].matches("^v[A-Z].*") || columns[i].matches("^V[A-Z].*")) {
        String icebergCol = "v_" + columns[i].substring(1).toLowerCase().replaceAll("\\$", "");
        selectBuffer.append("`").append(columns[i]).append("` AS ").append(icebergCol);
        columnList.add(icebergCol);
      } else {
        String icebergCol = columns[i].toLowerCase().replaceAll("\\$", "");
        selectBuffer.append("`").append(columns[i]).append("` AS ").append(icebergCol);
        columnList.add(icebergCol);
      }
      if (i < columns.length - 1) {
        selectBuffer.append(", ");
      }
    }

    // Generate a unique temporary table name
    String table = String.format("occurrence_%s_%d", datasetId.replace("-", "_"), attempt);

    // Switch to the configured Hive database
    spark.sql("USE " + config.getHiveDB());

    // Drop the table if it already exists
    spark.sql("DROP TABLE IF EXISTS " + table);

    // Check HDFS for remnant DB files from failed attempts
    Path warehousePath = new Path(config.getHdfsWarehousePath() + "/" + table);
    log.info("Checking warehouse path: {}", warehousePath);
    if (fs.exists(warehousePath)) {
      log.info("Deleting warehouse path: {}", warehousePath);
      fs.delete(warehousePath, true);
      log.info("Deletd warehouse path: {}", warehousePath);
    }
    hdfs.writeTo(table).create();

    log.info("Created Iceberg table: " + table);

    // Display table schema and initial record count
    spark.sql("DESCRIBE TABLE " + table).show(false);
    spark.sql("SELECT COUNT(*) FROM " + table).show(false);

    // Create or populate the occurrence table SQL
    spark.sql(occurrenceTableSQL);

    // Build the insert query
    String insertQuery =
        String.format(
            "INSERT OVERWRITE TABLE %s.occurrence (%s) SELECT %s FROM %s.%s",
            config.getHiveDB(),
            String.join(", ", columnList),
            selectBuffer,
            config.getHiveDB(),
            table);

    log.info("Inserting data into occurrence table: " + insertQuery);

    // Execute the insert
    spark.sql(insertQuery);

    // Drop the temporary table
    spark.sql("DROP TABLE " + table);

    log.info("Dropped Iceberg table: " + table);
    log.info("Checking warehouse path: {}", warehousePath);
    if (fs.exists(warehousePath)) {
      log.info("Deleting warehouse path: {}", warehousePath);
      fs.delete(warehousePath, true);
      log.info("Deletd warehouse path: {}", warehousePath);
    }

    spark.close();

    log.info("HDFS VIEW complete for dataset: " + datasetId);
  }

  static final String occurrenceTableSQL =
      """
      CREATE TABLE IF NOT EXISTS occurrence (
      gbifid STRING,
      v_accessrights STRING,
      v_bibliographiccitation STRING,
      v_language STRING,
      v_license STRING,
      v_modified STRING,
      v_publisher STRING,
      v_references STRING,
      v_rightsholder STRING,
      v_type STRING,
      v_institutionid STRING,
      v_collectionid STRING,
      v_datasetid STRING,
      v_institutioncode STRING,
      v_collectioncode STRING,
      v_datasetname STRING,
      v_ownerinstitutioncode STRING,
      v_basisofrecord STRING,
      v_informationwithheld STRING,
      v_datageneralizations STRING,
      v_dynamicproperties STRING,
      v_occurrenceid STRING,
      v_catalognumber STRING,
      v_recordnumber STRING,
      v_recordedby STRING,
      v_recordedbyid STRING,
      v_individualcount STRING,
      v_organismquantity STRING,
      v_organismquantitytype STRING,
      v_sex STRING,
      v_lifestage STRING,
      v_reproductivecondition STRING,
      v_caste STRING,
      v_behavior STRING,
      v_vitality STRING,
      v_establishmentmeans STRING,
      v_degreeofestablishment STRING,
      v_pathway STRING,
      v_georeferenceverificationstatus STRING,
      v_occurrencestatus STRING,
      v_preparations STRING,
      v_disposition STRING,
      v_associatedmedia STRING,
      v_associatedoccurrences STRING,
      v_associatedreferences STRING,
      v_associatedsequences STRING,
      v_associatedtaxa STRING,
      v_othercatalognumbers STRING,
      v_occurrenceremarks STRING,
      v_organismid STRING,
      v_organismname STRING,
      v_organismscope STRING,
      v_associatedorganisms STRING,
      v_previousidentifications STRING,
      v_organismremarks STRING,
      v_materialentityid STRING,
      v_materialentityremarks STRING,
      v_verbatimlabel STRING,
      v_materialsampleid STRING,
      v_eventid STRING,
      v_parenteventid STRING,
      v_eventtype STRING,
      v_fieldnumber STRING,
      v_eventdate STRING,
      v_eventtime STRING,
      v_startdayofyear STRING,
      v_enddayofyear STRING,
      v_year STRING,
      v_month STRING,
      v_day STRING,
      v_verbatimeventdate STRING,
      v_habitat STRING,
      v_samplingprotocol STRING,
      v_samplesizevalue STRING,
      v_samplesizeunit STRING,
      v_samplingeffort STRING,
      v_fieldnotes STRING,
      v_eventremarks STRING,
      v_locationid STRING,
      v_highergeographyid STRING,
      v_highergeography STRING,
      v_continent STRING,
      v_waterbody STRING,
      v_islandgroup STRING,
      v_island STRING,
      v_country STRING,
      v_countrycode STRING,
      v_stateprovince STRING,
      v_county STRING,
      v_municipality STRING,
      v_locality STRING,
      v_verbatimlocality STRING,
      v_minimumelevationinmeters STRING,
      v_maximumelevationinmeters STRING,
      v_verbatimelevation STRING,
      v_verticaldatum STRING,
      v_minimumdepthinmeters STRING,
      v_maximumdepthinmeters STRING,
      v_verbatimdepth STRING,
      v_minimumdistanceabovesurfaceinmeters STRING,
      v_maximumdistanceabovesurfaceinmeters STRING,
      v_locationaccordingto STRING,
      v_locationremarks STRING,
      v_decimallatitude STRING,
      v_decimallongitude STRING,
      v_geodeticdatum STRING,
      v_coordinateuncertaintyinmeters STRING,
      v_coordinateprecision STRING,
      v_pointradiusspatialfit STRING,
      v_verbatimcoordinates STRING,
      v_verbatimlatitude STRING,
      v_verbatimlongitude STRING,
      v_verbatimcoordinatesystem STRING,
      v_verbatimsrs STRING,
      v_footprintwkt STRING,
      v_footprintsrs STRING,
      v_footprintspatialfit STRING,
      v_georeferencedby STRING,
      v_georeferenceddate STRING,
      v_georeferenceprotocol STRING,
      v_georeferencesources STRING,
      v_georeferenceremarks STRING,
      v_geologicalcontextid STRING,
      v_earliesteonorlowesteonothem STRING,
      v_latesteonorhighesteonothem STRING,
      v_earliesteraorlowesterathem STRING,
      v_latesteraorhighesterathem STRING,
      v_earliestperiodorlowestsystem STRING,
      v_latestperiodorhighestsystem STRING,
      v_earliestepochorlowestseries STRING,
      v_latestepochorhighestseries STRING,
      v_earliestageorloweststage STRING,
      v_latestageorhigheststage STRING,
      v_lowestbiostratigraphiczone STRING,
      v_highestbiostratigraphiczone STRING,
      v_lithostratigraphicterms STRING,
      v_group STRING,
      v_formation STRING,
      v_member STRING,
      v_bed STRING,
      v_identificationid STRING,
      v_verbatimidentification STRING,
      v_identificationqualifier STRING,
      v_typestatus STRING,
      v_identifiedby STRING,
      v_identifiedbyid STRING,
      v_dateidentified STRING,
      v_identificationreferences STRING,
      v_identificationverificationstatus STRING,
      v_identificationremarks STRING,
      v_taxonid STRING,
      v_scientificnameid STRING,
      v_acceptednameusageid STRING,
      v_parentnameusageid STRING,
      v_originalnameusageid STRING,
      v_nameaccordingtoid STRING,
      v_namepublishedinid STRING,
      v_taxonconceptid STRING,
      v_scientificname STRING,
      v_acceptednameusage STRING,
      v_parentnameusage STRING,
      v_originalnameusage STRING,
      v_nameaccordingto STRING,
      v_namepublishedin STRING,
      v_namepublishedinyear STRING,
      v_higherclassification STRING,
      v_kingdom STRING,
      v_phylum STRING,
      v_class STRING,
      v_order STRING,
      v_superfamily STRING,
      v_family STRING,
      v_subfamily STRING,
      v_tribe STRING,
      v_subtribe STRING,
      v_genus STRING,
      v_genericname STRING,
      v_subgenus STRING,
      v_infragenericepithet STRING,
      v_specificepithet STRING,
      v_infraspecificepithet STRING,
      v_cultivarepithet STRING,
      v_taxonrank STRING,
      v_verbatimtaxonrank STRING,
      v_scientificnameauthorship STRING,
      v_vernacularname STRING,
      v_nomenclaturalcode STRING,
      v_taxonomicstatus STRING,
      v_nomenclaturalstatus STRING,
      v_taxonremarks STRING,
      identifiercount INT,
      crawlid INT,
      fragmentcreated BIGINT,
      xmlschema STRING,
      publishingorgkey STRING,
      unitqualifier STRING,
      networkkey ARRAY<STRING>,
      installationkey STRING,
      programmeacronym STRING,
      collectionkey STRING,
      institutionkey STRING,
      hostingorganizationkey STRING,
      isincluster BOOLEAN,
      dwcaextension ARRAY<STRING>,
      datasettitle STRING,
      eventdategte BIGINT,
      eventdatelte BIGINT,
      parenteventgbifid ARRAY<STRUCT<id: STRING,eventType: STRING>>,
      classifications MAP<STRING, ARRAY<STRING>>,
      classificationdetails MAP<STRING, MAP<STRING, STRING>>,
      accessrights STRING,
      bibliographiccitation STRING,
      language STRING,
      license STRING,
      modified BIGINT,
      publisher STRING,
      references STRING,
      rightsholder STRING,
      type STRING,
      institutionid STRING,
      collectionid STRING,
      datasetid ARRAY<STRING>,
      institutioncode STRING,
      collectioncode STRING,
      datasetname ARRAY<STRING>,
      ownerinstitutioncode STRING,
      basisofrecord STRING,
      informationwithheld STRING,
      datageneralizations STRING,
      dynamicproperties STRING,
      occurrenceid STRING,
      catalognumber STRING,
      recordnumber STRING,
      recordedby ARRAY<STRING>,
      recordedbyid ARRAY<STRING>,
      individualcount INT,
      organismquantity DOUBLE,
      organismquantitytype STRING,
      sex STRUCT<concept: STRING,lineage: ARRAY<STRING>>,
      lifestage STRUCT<concept: STRING,lineage: ARRAY<STRING>>,
      reproductivecondition STRING,
      caste STRING,
      behavior STRING,
      vitality STRING,
      establishmentmeans STRUCT<concept: STRING,lineage: ARRAY<STRING>>,
      degreeofestablishment STRUCT<concept: STRING,lineage: ARRAY<STRING>>,
      pathway STRUCT<concept: STRING,lineage: ARRAY<STRING>>,
      georeferenceverificationstatus STRING,
      occurrencestatus STRING,
      preparations ARRAY<STRING>,
      disposition STRING,
      associatedoccurrences STRING,
      associatedreferences STRING,
      associatedsequences ARRAY<STRING>,
      associatedtaxa STRING,
      othercatalognumbers ARRAY<STRING>,
      occurrenceremarks STRING,
      organismid STRING,
      organismname STRING,
      organismscope STRING,
      associatedorganisms STRING,
      previousidentifications STRING,
      organismremarks STRING,
      materialentityid STRING,
      materialentityremarks STRING,
      verbatimlabel STRING,
      materialsampleid STRING,
      eventid STRING,
      parenteventid STRING,
      eventtype STRUCT<concept: STRING,lineage: ARRAY<STRING>>,
      fieldnumber STRING,
      eventdate STRING,
      eventtime STRING,
      startdayofyear INT,
      enddayofyear INT,
      year INT,
      month INT,
      day INT,
      verbatimeventdate STRING,
      habitat STRING,
      samplingprotocol ARRAY<STRING>,
      samplesizevalue DOUBLE,
      samplesizeunit STRING,
      samplingeffort STRING,
      fieldnotes STRING,
      eventremarks STRING,
      locationid STRING,
      highergeographyid STRING,
      highergeography ARRAY<STRING>,
      continent STRING,
      waterbody STRING,
      islandgroup STRING,
      island STRING,
      countrycode STRING,
      stateprovince STRING,
      county STRING,
      municipality STRING,
      locality STRING,
      verbatimlocality STRING,
      verbatimelevation STRING,
      verticaldatum STRING,
      verbatimdepth STRING,
      minimumdistanceabovesurfaceinmeters STRING,
      maximumdistanceabovesurfaceinmeters STRING,
      locationaccordingto STRING,
      locationremarks STRING,
      decimallatitude DOUBLE,
      decimallongitude DOUBLE,
      coordinateuncertaintyinmeters DOUBLE,
      coordinateprecision DOUBLE,
      pointradiusspatialfit STRING,
      verbatimcoordinatesystem STRING,
      verbatimsrs STRING,
      footprintwkt STRING,
      footprintsrs STRING,
      footprintspatialfit STRING,
      georeferencedby ARRAY<STRING>,
      georeferenceddate STRING,
      georeferenceprotocol STRING,
      georeferencesources STRING,
      georeferenceremarks STRING,
      geologicalcontextid STRING,
      earliesteonorlowesteonothem STRUCT<concept: STRING,lineage: ARRAY<STRING>>,
      latesteonorhighesteonothem STRUCT<concept: STRING,lineage: ARRAY<STRING>>,
      earliesteraorlowesterathem STRUCT<concept: STRING,lineage: ARRAY<STRING>>,
      latesteraorhighesterathem STRUCT<concept: STRING,lineage: ARRAY<STRING>>,
      earliestperiodorlowestsystem STRUCT<concept: STRING,lineage: ARRAY<STRING>>,
      latestperiodorhighestsystem STRUCT<concept: STRING,lineage: ARRAY<STRING>>,
      earliestepochorlowestseries STRUCT<concept: STRING,lineage: ARRAY<STRING>>,
      latestepochorhighestseries STRUCT<concept: STRING,lineage: ARRAY<STRING>>,
      earliestageorloweststage STRUCT<concept: STRING,lineage: ARRAY<STRING>>,
      latestageorhigheststage STRUCT<concept: STRING,lineage: ARRAY<STRING>>,
      lowestbiostratigraphiczone STRING,
      highestbiostratigraphiczone STRING,
      lithostratigraphicterms STRING,
      `group` STRING,
      formation STRING,
      member STRING,
      bed STRING,
      identificationid STRING,
      verbatimidentification STRING,
      identificationqualifier STRING,
      typestatus STRUCT<concepts: ARRAY<STRING>,lineage: ARRAY<STRING>>,
      identifiedby ARRAY<STRING>,
      identifiedbyid ARRAY<STRING>,
      dateidentified BIGINT,
      identificationreferences STRING,
      identificationverificationstatus STRING,
      identificationremarks STRING,
      taxonid STRING,
      scientificnameid STRING,
      acceptednameusageid STRING,
      parentnameusageid STRING,
      originalnameusageid STRING,
      nameaccordingtoid STRING,
      namepublishedinid STRING,
      taxonconceptid STRING,
      scientificname STRING,
      acceptednameusage STRING,
      parentnameusage STRING,
      originalnameusage STRING,
      nameaccordingto STRING,
      namepublishedin STRING,
      namepublishedinyear STRING,
      higherclassification STRING,
      kingdom STRING,
      phylum STRING,
      class STRING,
      `order` STRING,
      superfamily STRING,
      family STRING,
      subfamily STRING,
      tribe STRING,
      subtribe STRING,
      genus STRING,
      genericname STRING,
      subgenus STRING,
      infragenericepithet STRING,
      specificepithet STRING,
      infraspecificepithet STRING,
      cultivarepithet STRING,
      taxonrank STRING,
      verbatimtaxonrank STRING,
      vernacularname STRING,
      nomenclaturalcode STRING,
      taxonomicstatus STRING,
      nomenclaturalstatus STRING,
      taxonremarks STRING,
      publishingcountry STRING,
      lastinterpreted BIGINT,
      elevation DOUBLE,
      elevationaccuracy DOUBLE,
      depth DOUBLE,
      depthaccuracy DOUBLE,
      distancefromcentroidinmeters DOUBLE,
      issue ARRAY<STRING>,
      taxonomicissue STRING,
      nontaxonomicissue STRING,
      mediatype ARRAY<STRING>,
      hascoordinate BOOLEAN,
      hasgeospatialissues BOOLEAN,
      checklistkey ARRAY<STRING>,
      taxonkey STRING,
      acceptedtaxonkey STRING,
      kingdomkey STRING,
      phylumkey STRING,
      classkey STRING,
      orderkey STRING,
      familykey STRING,
      genuskey STRING,
      subgenuskey STRING,
      specieskey STRING,
      species STRING,
      acceptedscientificname STRING,
      typifiedname STRING,
      protocol STRING,
      lastparsed BIGINT,
      lastcrawled BIGINT,
      isinvasive STRING,
      repatriated BOOLEAN,
      relativeorganismquantity DOUBLE,
      projectid ARRAY<STRING>,
      issequenced BOOLEAN,
      gbifregion STRING,
      publishedbygbifregion STRING,
      geologicaltime STRUCT<gt: DOUBLE,lte: DOUBLE>,
      lithostratigraphy ARRAY<STRING>,
      biostratigraphy ARRAY<STRING>,
      dnasequenceid ARRAY<STRING>,
      level0gid STRING,
      level0name STRING,
      level1gid STRING,
      level1name STRING,
      level2gid STRING,
      level2name STRING,
      level3gid STRING,
      level3name STRING,
      iucnredlistcategory STRING,
      ext_multimedia STRING,
      datasetkey STRING
    )
    USING iceberg
    PARTITIONED BY (datasetkey)
    TBLPROPERTIES (
      'write.format.default'='parquet',
      'parquet.compression'='SNAPPY',
      'auto.purge'='true'
    )""";
}
