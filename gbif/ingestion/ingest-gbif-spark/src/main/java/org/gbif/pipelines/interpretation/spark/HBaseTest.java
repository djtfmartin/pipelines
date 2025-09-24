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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.gbif.kvs.SaltedKeyGenerator;

/** A test harness for loading up HBase reads. Reads a CSV containing lat/lng pairs, */
public class HBaseTest implements Serializable {
  static final StructType INPUT_SCHEMA =
      new StructType(
          new StructField[] {
            new StructField("lat", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("lng", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("uncertainty", DataTypes.DoubleType, true, Metadata.empty())
          });

  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder().appName("HBase performance test").getOrCreate();

    // read the input
    Dataset<Row> records = spark.read().text(args[0]);

    JavaRDD<String> lines = records.toJavaRDD().map(row -> row.getString(0));

    // Open an HBase connection per partition, and do a lookup
    // (Added to test the performance without using GBIF KV code - it was the same)
    JavaRDD<String> processed =
        lines.mapPartitions(
            (FlatMapFunction<Iterator<String>, String>)
                iterator -> {
                  List<String> results = new ArrayList<>();

                  // Connect to HBase
                  Configuration hbaseConfig = HBaseConfiguration.create();
                  hbaseConfig.set(
                      "hbase.zookeeper.quorum",
                      "c5n7.gbif-test.org:31706,c5n9.gbif-test.org:31706,c6n8.gbif-test.org:31706");
                  hbaseConfig.set(
                      "zookeeper.znode.parent",
                      "/znode-93f9cdb5-d146-46da-9f80-e8546468b0fe/hbase");
                  Connection connection = ConnectionFactory.createConnection(hbaseConfig);
                  SaltedKeyGenerator saltedKeyGenerator = new SaltedKeyGenerator(10);

                  while (iterator.hasNext()) {
                    String line = iterator.next();

                    if (line.startsWith("decimallatitude")) continue;

                    String[] parts = line.split("\t");
                    Double lat = Double.parseDouble(parts[0]);
                    Double lng = Double.parseDouble(parts[1]);
                    Double uncertainty = parts.length > 2 ? Double.parseDouble(parts[2]) : null;
                    String key =
                        uncertainty == null
                            ? lat.toString() + '|' + lng
                            : lat.toString() + '|' + lng + '|' + uncertainty;

                    try (Table table = connection.getTable(TableName.valueOf("tim_geo_kv"))) {
                      byte[] saltedKey = saltedKeyGenerator.computeKey(Bytes.toBytes(key));
                      Get get = new Get(saltedKey);
                      Result result = table.get(get);
                      byte[] jsonBytes = result.getValue(Bytes.toBytes("v"), Bytes.toBytes("j"));
                      if (jsonBytes != null) {
                        results.add(line + "\t" + "bingo");
                      }
                    }

                    results.add(line.toUpperCase());
                  }

                  connection.close();

                  return results.iterator();
                });
    Dataset<String> countries = spark.createDataset(processed.rdd(), Encoders.STRING());

    // lookup in HBase and add a country using GBIF Code
    /*
    Dataset<String> countries =
        records.map(
            (MapFunction<Row, String>)
                row -> {
                  String line = row.getAs(0);

                  if (line.startsWith("decimallatitude")) return null;
                  String[] parts = line.split("\t");
                  Double lat = Double.parseDouble(parts[0]);
                  Double lng = Double.parseDouble(parts[1]);
                  Double uncertainty = parts.length > 2 ? Double.parseDouble(parts[2]) : null;

                  // Double lat = row.getAs("lat");
                  // Double lng = row.getAs("lng");
                  // Double uncertainty = row.getAs("uncertainty");

                  GeocodeRequest req =
                      uncertainty == null
                          ? GeocodeRequest.create(lat, lng)
                          : GeocodeRequest.create(lat, lng, uncertainty);
                  String country = GeoLookup.country(req); // lookup in HBase
                  return lat + "," + lng + "," + uncertainty + "," + country;
                },
            Encoders.STRING());
       */

    // write the output
    countries.write().mode(SaveMode.Overwrite).csv(args[1]);

    spark.close();
    System.exit(0);
  }
}
