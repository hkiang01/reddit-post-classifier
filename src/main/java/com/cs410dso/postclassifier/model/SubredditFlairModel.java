package com.cs410dso.postclassifier.model;

import com.cs410dso.postclassifier.ingestion.FilteredSubredditIngestion;

import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Collection;

/**
 * Created by harry on 11/28/16.
 */
public class SubredditFlairModel extends FilteredSubredditIngestion {

    /**
     * The {@link SparkSession} that the SubredditFlairModel operates off of
     */
    private SparkSession spark;

    /**
     * Instantiates a new SubredditFlairModel using 25 ingestion attempts from the front page
     */
    public SubredditFlairModel(SparkSession sparkSession) {
        super();
        // the action to scrape and ingest
        if(this.isDataEmpty()) {
            this.saveSubmissionAndMetadataAboveThresholdAsJson();
        }
        this.spark = sparkSession;
    }

    /**
     * Instantiates a new SubredditFlairModel
     * @param subreddits A {@link Collection} of subreddits to ingest
     * @param limit The number of {@link net.dean.jraw.models.Submission}s to ingest and filter through
     */
    public SubredditFlairModel(SparkSession sparkSession, Collection<String> subreddits, int limit) {
        super(subreddits, limit);
        // the action to scrape and ingest
        if(this.isDataEmpty()) {
            this.saveSubmissionAndMetadataAboveThresholdAsJson();
        }
        this.spark = sparkSession;
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
