package com.cs410dso.postclassifier;

import com.cs410dso.postclassifier.model.SubredditFlairModel;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.sql.*;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import scala.Function1;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterable;

/**
 * App main
 */
public class App {
    public static void main(String[] args) {

        // Spark setup
        SparkSession spark = SparkSession.builder().appName("Reddit Post Classifier").master("local[4]").getOrCreate();
        // https://stackoverflow.com/questions/31951728/how-to-set-up-logging-level-for-spark-application-in-intellij-idea
        LogManager.getRootLogger().setLevel(Level.ERROR); // hide INFO

        // scrape and ingest
        ArrayList<String> listOfSubreddits = new ArrayList();
        listOfSubreddits.add("machinelearning");
        SubredditFlairModel subredditFlairModel = new SubredditFlairModel(spark, listOfSubreddits, 1000);
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
        spark.sqlContext().dropTempTable("data");
        flairAndConcatText.show();

        // get the words out
        // https://spark.apache.org/docs/latest/ml-features.html#tokenizer
        // `\\W` pattern is a nonword character: [^A-Za-z0-9_] (see https://www.tutorialspoint.com/scala/scala_regular_expressions.htm)
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

        // https://stackoverflow.com/questions/34423281/spark-dataframe-word-count-per-document-single-row-per-document
        final CountVectorizerModel cvModel = new CountVectorizer().setInputCol("words").setOutputCol("features").fit(flairAndWords);
        final Dataset<Row> counted = cvModel.transform(flairAndWords);
        counted.printSchema();
        counted.show();

        // debugging
        int vocabLength = cvModel.vocabulary().length;
        System.out.println("vocab length: " + vocabLength);

        // the UDF for word counts
        final String[] vocabulary = cvModel.vocabulary();
        UDF1 wordFreqFromCountVectorizerModel = new UDF1<SparseVector, Map<String, Double>>() {
            public Map<String, Double> call(SparseVector v) {
                SparseVector sv = v.toSparse();
                int length = sv.indices().length;
                final double[] values = sv.values();
                Map<String, Double> myMap = new HashMap<>();
                for(int i = 0; i < length; i++) {
                    myMap.put(vocabulary[i], values[i]);
                }
                return myMap;
            }
        };

        // the SQL query for word counts
        counted.registerTempTable("counted");
        spark.sqlContext().udf().register("wordFreqFromCountVectorizerModel", wordFreqFromCountVectorizerModel, DataTypes.createMapType(DataTypes.StringType, DataTypes.DoubleType));
        final Dataset<Row> withFreq = spark.sql(
                "SELECT " +
                        "flair, " +
                        "concat_text, " +
                        "words, " +
                        "features, " +
                        "wordFreqFromCountVectorizerModel(features) AS freq " +
                        "FROM counted "
        );
        spark.sqlContext().dropTempTable("counted");
        // resultant word count
        withFreq.printSchema();
        withFreq.show();

        /**
         * Example:
         +-----+--------------------+--------------------+--------------------+--------------------+
         |flair|         concat_text|               words|            features|                freq|
         +-----+--------------------+--------------------+--------------------+--------------------+
         |  two|Some of the lates...|[some, of, the, l...|(24211,[0,1,2,3,4...|Map(gans -> 9.0, ...|
         | null|Hi, so I actually...|[hi, so, i, actua...|(24211,[0,1,2,3,4...|Map(serious -> 3....|
         | four|This is a TensorF...|[this, is, a, ten...|(24211,[0,1,2,3,4...|Map(serious -> 1....|
         |  one|I have implemente...|[i, have, impleme...|(24211,[0,1,2,3,4...|Map(gans -> 5.0, ...|
         |three|DataGenCARS is a ...|[datagencars, is,...|(24211,[0,1,2,3,4...|Map(mikhailfranco...|
         +-----+--------------------+--------------------+--------------------+--------------------+
         */

        UDF1 statisicalLMFromWordFreq = new UDF1<scala.collection.immutable.HashMap<String, Double>, scala.collection.immutable.HashMap<String, Double>>() {
            public scala.collection.immutable.HashMap<String, Double> call(scala.collection.immutable.HashMap<String, Double> wordFreq) {
//                final double sum = wordFreq.values().stream().mapToDouble(Double::doubleValue).sum();
//                Map<String, Double> myMap = new HashMap<String, Double>();
//                wordFreq.forEach( (word, freq) -> {
//                    myMap.put(word, freq/sum);
//                });
//                return myMap;
//                final Double sum = wordFreq.values().reduce((Double e1, Double e2) -> e1 + e2);
//                scala.collection.mutable.HashMap<String, Double> myMap = new scala.collection.mutable.HashMap<String, Double>();
//                wordFreq.keys().toList().foreach(new ForeachFunction<String>() {
//                    public void call(String word) {
//                        Double doubleOption = wordFreq.get(word).get();
//                        myMap.put()
//                    }
//                });
                return wordFreq;
            }
        };

        // the SQL query for word counts
        withFreq.registerTempTable("withFreq");
        spark.sqlContext().udf().register("statisicalLMFromWordFreq", statisicalLMFromWordFreq, DataTypes.createMapType(DataTypes.StringType, DataTypes.DoubleType));
        final Dataset<Row> withSLM = spark.sql(
                "SELECT " +
                        "flair, " +
                        "concat_text, " +
                        "words, " +
                        "features, " +
                        "freq, " +
                        "statisicalLMFromWordFreq(freq) AS statistical_lm " +
                        "FROM withFreq "
        );
        spark.sqlContext().dropTempTable("withFreq");
        // resultant word count
        withSLM.printSchema();
        withSLM.show();
    }

}
