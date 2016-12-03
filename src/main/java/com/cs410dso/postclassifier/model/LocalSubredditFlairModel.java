package com.cs410dso.postclassifier.model;

import com.cs410dso.postclassifier.ingestion.FilteredSubredditIngestion;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;

/**
 * Use this class over SubredditFlairModel if you're not connected to the Internet or only want to access the file under JSON_PATH
 */
public class LocalSubredditFlairModel {

    /**
     * The {@link SparkSession} that the SubredditFlairModel operates off of
     */
    private SparkSession spark;

    private static final String JSON_PATH = "./data.json";

    /**
     * Instantiates a new SubredditFlairModel using 25 ingestion attempts from the front page
     */
    public LocalSubredditFlairModel(SparkSession sparkSession) {
        // the action to scrape and ingest
        this.spark = sparkSession;
    }

//    /**
//     * Instantiates a new SubredditFlairModel
//     * @param subreddits A {@link Collection} of subreddits to ingest
//     * @param limit The number of {@link net.dean.jraw.models.Submission}s to ingest and filter through
//     */
//    public LocalSubredditFlairModel(SparkSession sparkSession, Collection<String> subreddits, int limit) {
//        this.spark = sparkSession;
//    }

    /**
     * Gets the path to JSON_PATH readable by Spark using Spark 2.0.2's spark.read.json([the path])
     * @return the path
     */
    public String getSparkDataPath() {
        String path = "";
        try {
            path = "file://" + Paths.get(JSON_PATH).toRealPath().toString();
        } catch (IOException e){
            e.printStackTrace();
        }
        return path;
    }

    /**
     * Gets the raw data (key, flair, text) from the posts above TEXT_THRESHOLD as specified in FilteredSubredditIngestion
     * @return the raw data with a key, flair, and text
     */
    public Dataset<Row> getRawDataset() {
        String path = this.getSparkDataPath();
        return spark.read().json(path).cache();
    }

    /**
     * Processes words such that they are all lowercase and white space is removed
     * @return the dataframe with key, flair, text, and words
     * @see <a href="https://spark.apache.org/docs/latest/ml-features.html#tokenizer">https://spark.apache.org/docs/latest/ml-features.html#tokenizer</a>
     */
    public Dataset<Row> getProcessedWords() {
        Dataset<Row> raw = this.getRawDataset();
        Tokenizer tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words");
        return tokenizer.transform(raw);
    }

}
