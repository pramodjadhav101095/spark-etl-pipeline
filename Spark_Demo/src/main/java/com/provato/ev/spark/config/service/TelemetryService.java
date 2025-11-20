package com.provato.ev.spark.config.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Service;

@Service
public class TelemetryService {

	private final SparkSession spark;

    public TelemetryService(SparkSession spark) {
        this.spark = spark;
    }

    public List<Map<String, Object>> processJson() {

        Dataset<Row> df = spark.read()
                .option("inferSchema", true)
                .option("multiline", true)
                .json("/app/data/telemetry.json");

        Dataset<Row> grouped = df.groupBy("stationId")
                .agg(functions.sum("energy").alias("totalEnergy"));

        // This is the Spark ACTION (UI will start here)
        List<Row> rows = grouped.collectAsList();

        // ⭐ KEEP SPARK UI ALIVE
        try {
            System.out.println("Keeping Spark UI alive...");
            Thread.sleep(20000);   // 20 seconds
        } catch (Exception e) {
            e.printStackTrace();
        }

        List<Map<String, Object>> result = new ArrayList<>();
        for (Row r : rows) {
            Map<String, Object> map = new HashMap<>();
            map.put("stationId", r.getAs("stationId"));
            map.put("totalEnergy", r.getAs("totalEnergy"));
            result.add(map);
        }

        return result;
    }
}