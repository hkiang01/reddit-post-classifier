package com.cs410dso.postclassifier;

import com.cs410dso.postclassifier.ingestion.FilteredSubredditIngestion;
import com.cs410dso.postclassifier.model.SubredditFlairModel;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Function1;
import scala.runtime.BoxedUnit;

/**
 * App main
 */
public class App {
    public static void main(String[] args) {


        // Spark setup
        SparkSession spark = SparkSession.builder().appName("Reddit Post Classifier").master("local[4]").getOrCreate();

        // scrape and ingest
        List<String> listOfSubreddits = new ArrayList<>();
        listOfSubreddits.add("machinelearning");
        SubredditFlairModel subredditFlairModel = new SubredditFlairModel(spark, listOfSubreddits, 1000);
        Dataset<Row> data = subredditFlairModel.getProcessedWords();
        data.printSchema();
        data.show();
        System.out.println("number of entries: " + Long.toString(data.count()));
        data.limit(1).toJavaRDD().foreach(f -> {
            for(int i = 0; i < f.size(); i++) {
                System.out.println(f.get(i).toString());
            }
        });
//
//        List<Row> data = Arrays.asList(
//                RowFactory.create(0, "Hi I heard about Spark"),
//                RowFactory.create(1, "I wish Java could use case classes"),
//                RowFactory.create(2, "Logistic,regression,models,are,neat")
//        );
//
//        StructType schema = new StructType(new StructField[]{
//                new StructField("label", DataTypes.IntegerType, false, Metadata.empty()),
//                new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
//        });
//
//        Dataset<Row> sentenceDataFrame = spark.createDataFrame(data, schema);
//
//        Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
//
//        Dataset<Row> wordsDataFrame = tokenizer.transform(sentenceDataFrame);
//        for (Row r : wordsDataFrame.select("words", "label").takeAsList(3)) {
//            java.util.List<String> words = r.getList(0);
//            for (String word : words) System.out.print(word + " ");
//            System.out.println();
//        }
//
//        RegexTokenizer regexTokenizer = new RegexTokenizer()
//                .setInputCol("sentence")
//                .setOutputCol("words")
//                .setPattern("\\W");  // alternatively .setPattern("\\w+").setGaps(false);
    }
}
