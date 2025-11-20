package com.provato.ev.spark.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkSessionConfig {

	
	
	@Bean
    public SparkSession sparkSession() {
        return SparkSession.builder()
                .appName("spark-spring-cluster-demo")
                .master("spark://spark-master:7077")
                .config("spark.ui.enabled", "true")
                .config("spark.ui.port", "4040")
                .config("spark.driver.bindAddress", "0.0.0.0")
                .config("spark.driver.host", "spring-app")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();
    }
}
