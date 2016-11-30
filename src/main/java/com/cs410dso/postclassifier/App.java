package com.cs410dso.postclassifier;

import com.cs410dso.postclassifier.model.LocalSubredditFlairModel;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import java.util.List;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
/**
 * App main
 */
public class App {
    public static void main(String[] args) {


        // Spark setup
        SparkSession spark = SparkSession.builder().appName("Reddit Post Classifier").master("local[4]").getOrCreate();

        // scrape and ingest
        LocalSubredditFlairModel subredditFlairModel = new LocalSubredditFlairModel(spark);
        Dataset<Row> data = subredditFlairModel.getProcessedWords();
        data.printSchema();
        data.show();
        System.out.println("number of entries: " + Long.toString(data.count()));

        // what flairs do we have?
        Dataset<Row> flairsDS = data.select("flair").dropDuplicates();
        List<String> flairs = flairsDS.toJavaRDD().map(new Function<Row, String>() {
            public String call(Row row) {
                return row.toString();
            }
        }).collect();
        flairs.forEach(f -> System.out.println(f));

        // for each flair, get the concatenated text from all posts
        // https://stackoverflow.com/questions/34150547/spark-group-concat-equivalent-in-scala-rdd
        // https://spark.apache.org/docs/2.0.2/api/java/org/apache/spark/sql/functions.html#concat_ws(java.lang.String,%20org.apache.spark.sql.Column...)
        data.registerTempTable("data");
        final Dataset<Row> flairAndConcatText = spark.sql(
                "SELECT " +
                        "flair, " +
                        "concat_ws( ' ', collect_list(text)) AS concat_text " +
                        "FROM data " +
                        "GROUP BY flair");
        flairAndConcatText.show();

        // get the words out
        // https://spark.apache.org/docs/latest/ml-features.html#tokenizer
        // `\\W` pattern is a nonword character: [^A-Za-z0-9_]
        // this transform also forces lower case
        RegexTokenizer tokenizer = new RegexTokenizer().setInputCol("concat_text").setOutputCol("words").setPattern("\\W");
        final Dataset<Row> flairAndWords = tokenizer.transform(flairAndConcatText);

        flairAndWords.printSchema();
        flairAndWords.show();
        /**
         * Example:
         * +---------------+--------------------+--------------------+
         |          flair|         concat_text|               words|
         +---------------+--------------------+--------------------+
         | one	Discussion|I need help findi...|[i, need, help, f...|
         | three	Research| 5 algorithms to ...|[5, algorithms, t...|
         |one	Discusssion|For example, if y...|[for, example, if...|
         |   four	Project| ヤロミル about AI Sk...|[about, ai, skip,...|
         |       two	News| Home Moments Sea...|[home, moments, s...|
         |      null	null|My data:
         I am usi...|[my, data, i, am,...|
         +---------------+--------------------+--------------------+
         */



    }
}
