package com.cs410dso.postclassifier;

import com.cs410dso.postclassifier.ingestion.FilteredSubredditIngestion;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
/**
 * App main
 */
public class App {
    public static void main(String[] args) {

        // scrape and ingest
        List<String> listOfSubreddits = new ArrayList<>();
        listOfSubreddits.add("machinelearning");
        FilteredSubredditIngestion ingestion = new FilteredSubredditIngestion(listOfSubreddits, 1000);

        // the action to scrape and ingest
        if(ingestion.isDataEmpty()) {
            ingestion.saveSubmissionAndMetadataAboveThresholdAsJson();
        }

        // Spark setup
        SparkSession spark = SparkSession.builder().appName("Reddit Post Classifier").master("local[4]").getOrCreate();

        // read into dataframe
        String path = ingestion.getSparkDataPath();
        Dataset<Row> data = spark.read().json(path).cache();
        data.printSchema();
        data.show();
        System.out.println("number of entries: " + Long.toString(data.count()));
    }
}
