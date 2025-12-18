package org.gbif.pipelines.interpretation.spark;

import static org.apache.spark.sql.functions.col;
import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;
import static org.gbif.pipelines.interpretation.MetricsUtil.writeMetricsYaml;
import static org.gbif.pipelines.interpretation.spark.SparkUtil.getFileSystem;
import static org.gbif.pipelines.interpretation.spark.SparkUtil.getSparkSession;
import static org.gbif.pipelines.interpretation.standalone.DistributedUtil.timeAndRecPerSecond;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.jetbrains.annotations.NotNull;
import org.slf4j.MDC;

/**
 * This pipeline loads the /hdfs directory for a dataset/attempt, creates a temporary table with the
 * parquet, and then loads into the main occurrence table which is partitioned by datasetKey
 */
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

    @Parameter(names = "--tableName", description = "Table name", required = true)
    private String tableName = "occurrence";

    @Parameter(names = "--sourceDirectory", description = "Table name", required = true)
    private String sourceDirectory = "hdfs";

    @Parameter(names = "--config", description = "Path to YAML configuration file")
    private String config = "/tmp/pipelines-spark.yaml";

    @Parameter(
        names = "--master",
        description =
            "Spark master - there for local dev only. Use --master=local[*] to run locally.",
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
    jCommander.setAcceptUnknownOptions(true); // FIXME to ease airflow/registry integration
    jCommander.parse(argsv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    PipelinesConfig config = loadConfig(args.config);
    String datasetId = args.datasetId;
    int attempt = args.attempt;

    /* ############ standard init block ########## */
    SparkSession spark =
        getSparkSession(args.master, args.appName, config, TableBuild::configSparkSession);
    FileSystem fileSystem = getFileSystem(spark, config);

    /* ############ standard init block - end ########## */
    runTableBuild(
        spark,
        fileSystem,
        config,
        datasetId,
        attempt,
        args.tableName,
        args.sourceDirectory,
        OccurrenceHdfsRecord.class);

    spark.stop();
    spark.close();
    fileSystem.close();
    System.exit(0);
  }

  public static void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {
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
        .config("spark.sql.warehouse.dir", "hdfs://gbif-hdfs/stackable/warehouse")
        .config("spark.hadoop.hive.metastore.uris", config.getHiveMetastoreUris());
  }

  /**
   * Run an incremental table build for the supplied dataset
   *
   * @param spark Spark session
   * @param fileSystem HDFS file system
   * @param config Pipelines config
   * @param datasetId Dataset ID
   * @param attempt Attempt number
   * @param tableName table name e.g. occurrence or event
   * @param sourceDirectory Source directory of parquet files
   */
  public static <T> void runTableBuild(
      SparkSession spark,
      FileSystem fileSystem,
      PipelinesConfig config,
      String datasetId,
      int attempt,
      String tableName,
      String sourceDirectory,
      Class<T> recordClass)
      throws Exception {

    long start = System.currentTimeMillis();
    MDC.put("datasetKey", datasetId);
    log.info("Starting tablebuild");

    String outputPath = String.format("%s/%s/%d", config.getOutputPath(), datasetId, attempt);

    // load hdfs view
    Dataset<Row> hdfs = spark.read().parquet(outputPath + "/" + sourceDirectory);

    // Generate a unique temporary table name
    String table = String.format("%s_%s_%d", tableName, datasetId.replace("-", "_"), attempt);

    // Switch to the configured Hive database
    spark.sql("USE " + config.getHiveDB());

    // Drop the table if it already exists
    spark.sql("DROP TABLE IF EXISTS " + table);

    // Check HDFS for remnant DB files from failed attempts
    cleanHdfsPath(fileSystem, config, table);
    hdfs.writeTo(table).create();

    log.debug("Created Iceberg table: {}", table);

    // Display table schema and initial record count
    Dataset<Row> result = spark.sql("SELECT COUNT(*) FROM " + table);
    long avroToHdfsCountAttempted = result.collectAsList().get(0).getLong(0);

    if (log.isDebugEnabled()) {
      spark.sql("DESCRIBE TABLE " + table).show(false);
      spark.sql("SELECT COUNT(*) FROM " + table).show(false);
    }

    Dataset<Row> df =
        spark.sql(
            "SELECT COUNT(*) AS cnt FROM "
                + table
                + " WHERE datasetKey IS NULL"
                + " OR datasetKey = ''"
                + " OR gbifId IS NULL"
                + " OR gbifId = ''");
    long count = df.collectAsList().get(0).getLong(0);
    if (count > 0) {
      log.warn(
          "There are {} records with NULL or empty datasetKey or gbifId in the temporary table {}",
          count,
          table);
      throw new IllegalStateException("There are " + count + " records with NULL datasetKey");
    }

    // Create or populate the occurrence table SQL
    spark.sql(getCreateTableSQL(tableName));

    // Read the target table i.e. 'occurrence' or 'event' schema to ensure it exists
    StructType tblSchema = spark.read().format("iceberg").load(tableName).schema();

    // get the hdfs columns from the parquet with mappings to iceberg columns
    Map<String, HdfsColumn> hdfsColumnList = getHdfsColumns(hdfs);

    // Build the insert query
    String insertQuery =
        String.format(
            "INSERT OVERWRITE TABLE %s.%s (%s) SELECT %s FROM %s.%s",
            config.getHiveDB(),
            tableName,
            Arrays.stream(tblSchema.fields())
                .map(StructField::name)
                .collect(Collectors.joining(", ")),
            generateSelectColumns(tblSchema, hdfsColumnList),
            config.getHiveDB(),
            table);

    log.debug("Inserting data into {}} table: {}", tableName, insertQuery);

    // Execute the insert
    spark.sql(insertQuery);

    // Drop the temporary table
    spark.sql("DROP TABLE " + table);

    log.debug("Dropped Iceberg table: {}", table);
    cleanHdfsPath(fileSystem, config, table);

    // 4. write metrics to yaml
    writeMetricsYaml(
        fileSystem,
        Map.of("avroToHdfsCountAttempted", avroToHdfsCountAttempted),
        outputPath + "/" + getMetricsFileName(tableName));

    log.info(timeAndRecPerSecond("tablebuild", start, avroToHdfsCountAttempted));
  }

  private static String generateSelectColumns(
      StructType tblSchema, Map<String, HdfsColumn> hdfsColumnList) {
    return Arrays.stream(tblSchema.fields())
        .map(
            structField -> {
              HdfsColumn hdfsColumn = hdfsColumnList.get(structField.name());
              if (hdfsColumn != null) {
                return hdfsColumn.select;
              } else {
                // Column not found in HDFS, select NULL with alias
                return "NULL AS `" + structField.name() + "`";
              }
            })
        .collect(Collectors.joining(", "));
  }

  @NotNull
  private static Map<String, HdfsColumn> getHdfsColumns(Dataset<Row> hdfs) {
    // get the hdfs columns from the parquet

    // map them to select statements
    Map<String, HdfsColumn> hdfsColumnList = new HashMap<>();

    for (String col : hdfs.columns()) {

      HdfsColumn hdfsColumn = new HdfsColumn();
      hdfsColumn.originalName = col;

      final String lower = col.toLowerCase().replace("$", "");

      if (col.equalsIgnoreCase("extMultimedia")) {

        hdfsColumn.icebergCol = "ext_multimedia";
        hdfsColumn.select = "`extMultimedia` AS `ext_multimedia`";

      } else if (col.equalsIgnoreCase("extHumboldt")) {

        hdfsColumn.icebergCol = "ext_humboldt";
        hdfsColumn.select = "`extHumboldt` AS `ext_humboldt`";

      } else if (col.matches("^[vV][A-Z].*")) {
        // Handles names like VSomething → v_something
        String normalized = "v_" + col.substring(1).toLowerCase().replace("$", "");
        hdfsColumn.icebergCol = normalized;
        hdfsColumn.select = "`" + col + "` AS " + normalized;

      } else {

        hdfsColumn.icebergCol = lower;
        hdfsColumn.select = "`" + col + "` AS " + lower;
      }

      hdfsColumnList.put(hdfsColumn.icebergCol, hdfsColumn);
    }

    return hdfsColumnList;
  }

  @NotNull
  public static String getMetricsFileName(String tableName) {
    return tableName + "-to-hdfs.yml";
  }

  @NotNull
  private static void cleanHdfsPath(FileSystem fileSystem, PipelinesConfig config, String table)
      throws IOException {
    Path warehousePath = new Path(config.getHdfsWarehousePath() + "/" + table);
    log.debug("Checking warehouse path: {}", warehousePath);
    if (fileSystem.exists(warehousePath)) {
      log.debug("Deleting warehouse path: {}", warehousePath);
      fileSystem.delete(warehousePath, true);
      log.debug("Deleted warehouse path: {}", warehousePath);
    }
  }

  // FIXME - the table definition needs to be loaded from elsewhere...
  static String getCreateTableSQL(String tableName) {
    return String.format(
        """
      CREATE TABLE IF NOT EXISTS %s (
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
      taxonomicstatuses MAP<STRING, STRING>,
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
      taxonomicissue MAP<STRING, ARRAY<STRING>>,
      nontaxonomicissue ARRAY<STRING>,
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
      superfamilykey STRING,
      familykey STRING,
      subfamilykey STRING,
      tribekey STRING,
      subtribekey STRING,
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
      ext_humboldt STRING,
      datasetkey STRING
    )
    USING iceberg
    PARTITIONED BY (datasetkey)
    TBLPROPERTIES (
      'write.format.default'='parquet',
      'parquet.compression'='SNAPPY',
      'auto.purge'='true'
    )""",
        tableName);
  }

  static class HdfsColumn {
    String originalName;
    String select;
    String icebergCol;
    Boolean existsInSource;
  }
}
