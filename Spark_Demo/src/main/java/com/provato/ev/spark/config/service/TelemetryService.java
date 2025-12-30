package com.provato.ev.spark.config.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;

/*Narrow Transformation: Added filter() on energy > 10 (processes locally, no data shuffle).
Wide Transformations: Kept groupBy().agg() (shuffles for aggregation) and added join()
                       with a static metadata DF (shuffles to match keys, enriches with stationName).

*/




@Service
public class TelemetryService {

    private final SparkSession spark;

    public TelemetryService(SparkSession spark) {
        this.spark = spark;
    }

    public List<Map<String, Object>> processJson() {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            // Read JSON (Narrow: file scan)
            Dataset<Row> df = spark.read()
                    .option("inferSchema", true)
                    .option("multiline", true)
                    .json("/app/data/telemetry.json");

            if (df.count() == 0) {
                Map<String, Object> errorMap = new HashMap<>();
                errorMap.put("error", "No data found in telemetry.json");
                result.add(errorMap);
                return result;
            }

            // Narrow Transformation: Filter energy > 10 (no shuffle, per-partition)
            Dataset<Row> filtered = df.filter(df.col("energy").gt(10));

            // Wide Transformation 1: GroupBy + Agg (shuffle for grouping)
            Dataset<Row> grouped = filtered.groupBy("stationId")
                    .agg(functions.sum("energy").alias("totalEnergy"));

            // Static Metadata DF (for join demo)
            StructType metadataSchema = new StructType(new StructField[] {
                new StructField("stationId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("stationName", DataTypes.StringType, false, Metadata.empty())
            });
            List<Row> metadataRows = List.of(
                RowFactory.create("ST001", "Downtown Charger"),
                RowFactory.create("ST002", "Highway Fast")
            );
            Dataset<Row> stationMetadata = spark.createDataFrame(metadataRows, metadataSchema);

            // Wide Transformation 2: Join on stationId (shuffle to co-locate data)
            Dataset<Row> enriched = grouped.join(stationMetadata, "stationId", "inner")
                    .select("stationId", "totalEnergy", "stationName");

            // Action: Collect (triggers all transformations)
            List<Row> rows = enriched.collectAsList();

            // Convert to maps
            for (Row r : rows) {
                Map<String, Object> map = new HashMap<>();
                map.put("stationId", r.getAs("stationId"));
                map.put("totalEnergy", r.getAs("totalEnergy"));
                map.put("stationName", r.getAs("stationName"));
                result.add(map);
            }

            // Optional: Brief pause for UI visibility (reduced from 20s)
            Thread.sleep(5000);

        } catch (Exception e) {
            e.printStackTrace();
            Map<String, Object> errorMap = new HashMap<>();
            errorMap.put("error", "Job failed: " + e.getMessage());
            errorMap.put("details", e.getClass().getSimpleName());
            result.add(errorMap);
        }
        return result;
    }
}