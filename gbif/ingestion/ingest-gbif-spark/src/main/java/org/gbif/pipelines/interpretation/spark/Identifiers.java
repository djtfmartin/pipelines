package org.gbif.pipelines.interpretation.spark;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;

import java.io.*;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Identifiers implements Serializable {

    public static void main(String[] args) throws IOException {

        if (args.length < 3) {
            System.err.println(
                    "Usage: java -jar ingest-gbif-spark-<version>.jar <config.yaml> <datasetID> <attempt>");
            System.exit(1);
        }

        PipelinesConfig config = loadConfig(args[0]);

        String datasetID = args[1];
        String attempt = args[2];
        String inputPath = config.getInputPath() + "/" + datasetID + "/" + attempt;
        String outputPath = config.getOutputPath() + "/" + datasetID + "/" + attempt;

        SparkSession spark =
                SparkSession.builder()
                        .appName("Run local spark")
                        .master("local[*]") // Use local mode with all cores
                        .getOrCreate();

        // Read the verbatim input
        Dataset<ExtendedRecord> records =
                spark.read().format("avro").load(inputPath)
                        .as(Encoders.bean(ExtendedRecord.class));

        // run the identifier transform
        Dataset<IdentifierRecord> identifiers = identifierTransform(records);

        // Write the identifiers to parquet
        identifiers.write().mode("overwrite").parquet(
                outputPath + "/identifiers");
    }

    private static Dataset<IdentifierRecord> identifierTransform(Dataset<ExtendedRecord> records) {

        // dummy implementation of identifier transform
        return records.map((MapFunction<ExtendedRecord, IdentifierRecord>)
                        er -> IdentifierRecord.newBuilder()
                                .setId(er.getId())
                                .setInternalId(er.getId()) //FIXME
                                .build(),
                Encoders.bean(IdentifierRecord.class));
    }

    public static PipelinesConfig loadConfig(String configPath) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(new File(configPath), UTF_8))) {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);

            SimpleModule keyTermDeserializer = new SimpleModule();
            keyTermDeserializer.addKeyDeserializer(
                    Term.class,
                    new KeyDeserializer() {
                        @Override
                        public Term deserializeKey(String value, DeserializationContext dc) {
                            return TermFactory.instance().findTerm(value);
                        }
                    });
            mapper.registerModule(keyTermDeserializer);

            mapper.findAndRegisterModules();
            return mapper.readValue(br, PipelinesConfig.class);
        } catch (IOException e) {
            System.err.println("Error reading config file: " + e.getMessage());
            throw new RuntimeException("Failed to load configuration", e);
        }
    }
}
