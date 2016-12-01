package com.cs410dso.postclassifier;

import com.cs410dso.postclassifier.model.SubredditFlairModel;

import com.cs410dso.postclassifier.model.TwoString;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.sql.*;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.expressions.Window;

import java.util.*;

import org.apache.spark.sql.types.StructType;
import org.codehaus.janino.Java;
import scala.Function1;
import scala.collection.Iterable;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import org.apache.spark.sql.SQLImplicits.*;

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

        // get the combined text of all posts
        final String allPostsText = flairAndConcatText.select("concat_text").toJavaRDD().map(new Function<Row, String>() {
            public String call(Row row) {
                return row.get(0).toString();
            }
        }).reduce(new Function2<String, String, String>() {
            public String call(String s1, String s2) {
                return s1 + " " + s2;
            }
        });

        // add background text as column
        final Dataset<Row> withBackgroundText = flairAndConcatText.withColumn("background_text", functions.lit(allPostsText));
        withBackgroundText.show();

        // get the background words out
        RegexTokenizer backgroundTokenizer = new RegexTokenizer().setInputCol("background_text").setOutputCol("background_words").setPattern("\\W");
        final Dataset<Row> withBackgroundWords = backgroundTokenizer.transform(withBackgroundText);
        withBackgroundWords.printSchema();
        withBackgroundWords.show();

        // get the words out
        // https://spark.apache.org/docs/latest/ml-features.html#tokenizer
        // `\\W` pattern is a nonword character: [^A-Za-z0-9_] (see https://www.tutorialspoint.com/scala/scala_regular_expressions.htm)
        // this transform also forces lower case
        RegexTokenizer tokenizer = new RegexTokenizer().setInputCol("concat_text").setOutputCol("words").setPattern("\\W");
        final Dataset<Row> flairAndWords = tokenizer.transform(withBackgroundWords);

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

        final CountVectorizerModel backgroundCVModel = new CountVectorizer().setInputCol("background_words").setOutputCol("background_features").fit(flairAndWords);
        final Dataset<Row> withBackgroundFeatures = backgroundCVModel.transform(flairAndWords);
        withBackgroundFeatures.printSchema();
        withBackgroundFeatures.show();

        // https://stackoverflow.com/questions/34423281/spark-dataframe-word-count-per-document-single-row-per-document
        final CountVectorizerModel cvModel = new CountVectorizer().setInputCol("words").setOutputCol("features").fit(withBackgroundFeatures);
        final Dataset<Row> counted = cvModel.transform(withBackgroundFeatures);
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
                        "wordFreqFromCountVectorizerModel(features) AS freq, " +
                        "background_text, " +
                        "background_words, " +
                        "background_features, " +
                        "wordFreqFromCountVectorizerModel(background_features) AS background_freq " +
                        "FROM counted "
        );
        spark.sqlContext().dropTempTable("counted");
        // resultant word count
        withFreq.printSchema();
        withFreq.show();


        /**
         * Example:


         +-----+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
         |flair|         concat_text|               words|            features|                freq|     background_text|    background_words| background_features|     background_freq|
         +-----+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
         |  two|Some of the lates...|[some, of, the, l...|(24368,[0,1,2,3,4...|Map(serious -> 1....|I have implemente...|[i, have, impleme...|(24368,[0,1,2,3,4...|Map(demsar -> 1.0...|
         | null|Hi, so I actually...|[hi, so, i, actua...|(24368,[0,1,2,3,4...|Map(serious -> 2....|I have implemente...|[i, have, impleme...|(24368,[0,1,2,3,4...|Map(demsar -> 1.0...|
         | four|This is a TensorF...|[this, is, a, ten...|(24368,[0,1,2,3,4...|Map(serious -> 1....|I have implemente...|[i, have, impleme...|(24368,[0,1,2,3,4...|Map(demsar -> 1.0...|
         |  one|I have implemente...|[i, have, impleme...|(24368,[0,1,2,3,4...|Map(serious -> 1....|I have implemente...|[i, have, impleme...|(24368,[0,1,2,3,4...|Map(demsar -> 1.0...|
         |three|DataGenCARS is a ...|[datagencars, is,...|(24368,[0,1,2,3,4...|Map(mikhailfranco...|I have implemente...|[i, have, impleme...|(24368,[0,1,2,3,4...|Map(demsar -> 1.0...|
         +-----+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
         */

        // UDF for statistical language model
        UDF1 statisicalLMFromWordFreq = new UDF1<scala.collection.immutable.HashMap<String, Double>, scala.collection.mutable.HashMap<String, Double>>() {
            public scala.collection.mutable.HashMap<String, Double> call(scala.collection.immutable.HashMap<String, Double> wordFreq) {
                List<Double> valuesList = JavaConversions.asJavaList(wordFreq.values().toList());
                Double sum = valuesList.stream().mapToDouble(Double::doubleValue).sum();
                scala.collection.mutable.HashMap<String, Double> myMap = new scala.collection.mutable.HashMap<String, Double>();
                final Map<String, Double> javaMap = JavaConversions.mapAsJavaMap(wordFreq);
                javaMap.keySet().forEach(word -> {
                    Double freq = javaMap.get(word);
                    Double newFreq = freq / sum; // the main logic
                    myMap.put(word, newFreq);
                });
                return myMap;
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
                        "statisicalLMFromWordFreq(freq) AS statistical_lm, " +
                        "background_text, " +
                        "background_words, " +
                        "background_features, " +
                        "background_freq, " +
                        "statisicalLMFromWordFreq(background_freq) AS background_statistical_lm " +
                        "FROM withFreq "
        );
        spark.sqlContext().dropTempTable("withFreq");
        // resultant word count
        withSLM.printSchema();
        withSLM.show();

        Iterator<Row> rowIterator = withSLM.toJavaRDD().toLocalIterator();
        Row row = rowIterator.next();
        System.out.println("flair: " + row.get(0));
        System.out.println("statistical_lm: " + row.get(5));
        System.out.println("background_statistical_lm: " + row.get(10));

    }

}
