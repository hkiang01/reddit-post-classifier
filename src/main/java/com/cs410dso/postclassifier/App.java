package com.cs410dso.postclassifier;

import com.cs410dso.postclassifier.model.LocalSubredditFlairModel;
import com.cs410dso.postclassifier.model.SubredditFlairModel;

import com.cs410dso.postclassifier.model.TwoString;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
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
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.expressions.Window;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

import org.apache.spark.sql.types.StructType;
import org.codehaus.janino.Java;
import scala.Array;
import scala.Function1;
import scala.Tuple2;
import scala.collection.Iterable;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import org.apache.spark.sql.SQLImplicits.*;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

/**
 * App main
 */
public class App {

    public static void main(String[] args) {

        // Spark setup
        SparkSession spark = SparkSession.builder().appName("Reddit Post Classifier").master("local[4]").getOrCreate();
        // https://stackoverflow.com/questions/31951728/how-to-set-up-logging-level-for-spark-application-in-intellij-idea
        LogManager.getRootLogger().setLevel(Level.ERROR); // hide INFO

        // scrape and in¡gest
        ArrayList<String> listOfSubreddits = new ArrayList();
        listOfSubreddits.add("machinelearning");
//        SubredditFlairModel subredditFlairModel = new SubredditFlairModel(spark, listOfSubreddits, 1000);
         LocalSubredditFlairModel subredditFlairModel = new LocalSubredditFlairModel(spark); // change to this if behind corporate firewall and you have data.json
        Dataset<Row> dataRawWithNull = subredditFlairModel.getProcessedWords();
        Dataset<Row> dataRaw = dataRawWithNull.where(dataRawWithNull.col("flair").isNotNull()); // filter out null flairs or flairs that don't have css
        dataRaw.printSchema();
        dataRaw.show();
        System.out.println("number of entries: " + Long.toString(dataRaw.count()));

        final CountVectorizerModel dataCVModel = new CountVectorizer().setInputCol("words").setOutputCol("words_features").fit(dataRaw);
        final Dataset<Row> withWordsFeatures = dataCVModel.transform(dataRaw);
        withWordsFeatures.printSchema();
        withWordsFeatures.show();

        // the UDF for word counts
        final String[] postVocab = dataCVModel.vocabulary();
        UDF1 postWordFreqFromCountVectorizerModel = new UDF1<SparseVector, Map<String, Double>>() {
            public Map<String, Double> call(SparseVector v) {
                SparseVector sv = v.toSparse();
                int length = sv.indices().length;
                final double[] values = sv.values();
                Map<String, Double> myMap = new HashMap<>();
                for(int i = 0; i < length; i++) {
                    myMap.put(postVocab[i], values[i]);
                }
                return myMap;
            }
        };
        spark.sqlContext().udf().register("postWordFreqFromCountVectorizerModel", postWordFreqFromCountVectorizerModel, DataTypes.createMapType(DataTypes.StringType, DataTypes.DoubleType));

        // the SQL query for word counts
        withWordsFeatures.registerTempTable("withWordsFeatures");
        final Dataset<Row> data = spark.sql(
                "SELECT " +
                        "author, " +
                        "created, " +
                        "flair, " +
                        "text, " +
                        "words, " +
                        "words_features, " +
                        "postWordFreqFromCountVectorizerModel(words_features) AS words_freq " +
                        "FROM withWordsFeatures"
        );
        spark.sqlContext().dropTempTable("withWordsFeatures");
        // resultant word count
        data.printSchema();
        data.show();

        // what flairs do we have?
        Dataset<Row> flairsDS = dataRaw.select("flair").dropDuplicates();
        List<String> flairs = flairsDS.toJavaRDD().map(new Function<Row, String>() {
            public String call(Row row) {
                return row.toString();
            }
        }).collect();
        flairs.forEach(f -> System.out.println(f));

        // for each flair, get the concatenated text from all posts
        // https://stackoverflow.com/questions/34150547/spark-group-concat-equivalent-in-scala-rdd
        // https://spark.apache.org/docs/2.0.2/api/java/org/apache/spark/sql/functions.html#concat_ws(java.lang.String,%20org.apache.spark.sql.Column...)
        dataRaw.registerTempTable("dataRaw");
        final Dataset<Row> flairAndConcatText = spark.sql(
                "SELECT " +
                        "flair, " +
                        "concat_ws( ' ', collect_list(text)) AS concat_text " +
                        "FROM dataRaw " +
                        "GROUP BY flair");
        spark.sqlContext().dropTempTable("dataRaw");
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
        spark.sqlContext().udf().register("wordFreqFromCountVectorizerModel", wordFreqFromCountVectorizerModel, DataTypes.createMapType(DataTypes.StringType, DataTypes.DoubleType));

        // debugging
        int vocabLength = cvModel.vocabulary().length;
        System.out.println("vocab length: " + vocabLength);

        // the SQL query for word counts
        counted.registerTempTable("counted");
        final Dataset<Row> withFreq = spark.sql(
                "SELECT " +
                        "flair, " +
                        "concat_text, " +
                        "words, " +
                        "features, " +
                        "wordFreqFromCountVectorizerModel(features) AS model_freq, " +
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
                        "model_freq, " +
                        "statisicalLMFromWordFreq(model_freq) AS statistical_lm, " +
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

        // ta-dah!
        Iterator<Row> rowIterator = withSLM.toJavaRDD().toLocalIterator();
        Row row = rowIterator.next();
        System.out.println("flair: " + row.get(0));
        System.out.println("statistical_lm: " + row.get(5));
        System.out.println("background_statistical_lm: " + row.get(10));

        /**
         * +-----+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+-------------------------+
         |flair|         concat_text|               words|            features|                freq|      statistical_lm|     background_text|    background_words| background_features|     background_freq|background_statistical_lm|
         +-----+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+-------------------------+
         |  two|Some of the lates...|[some, of, the, l...|(24368,[0,1,2,3,4...|Map(serious -> 1....|Map(serious -> 2....|Some of the lates...|[some, of, the, l...|(24368,[0,1,2,3,4...|Map(demsar -> 1.0...|     Map(demsar -> 1.9...|
         | null|Hi, so I actually...|[hi, so, i, actua...|(24368,[0,1,2,3,4...|Map(serious -> 3....|Map(serious -> 3....|Some of the lates...|[some, of, the, l...|(24368,[0,1,2,3,4...|Map(demsar -> 1.0...|     Map(demsar -> 1.9...|
         | four|This is a TensorF...|[this, is, a, ten...|(24368,[0,1,2,3,4...|Map(serious -> 2....|Map(serious -> 2....|Some of the lates...|[some, of, the, l...|(24368,[0,1,2,3,4...|Map(demsar -> 1.0...|     Map(demsar -> 1.9...|
         |  one|I have implemente...|[i, have, impleme...|(24368,[0,1,2,3,4...|Map(serious -> 1....|Map(serious -> 2....|Some of the lates...|[some, of, the, l...|(24368,[0,1,2,3,4...|Map(demsar -> 1.0...|     Map(demsar -> 1.9...|
         |three|DataGenCARS is a ...|[datagencars, is,...|(24368,[0,1,2,3,4...|Map(mikhailfranco...|Map(mikhailfranco...|Some of the lates...|[some, of, the, l...|(24368,[0,1,2,3,4...|Map(demsar -> 1.0...|     Map(demsar -> 1.9...|
         +-----+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+-------------------------+

         flair: two
         statistical_lm: Map(serious -> 2.5887286753475367E-5, gans -> 1.2943643376737683E-4, k40 -> 2.5887286753475367E-5, subreddit -> 2.5887286753475367E-5, ...
         background_statistical_lm: Map(demsar -> 1.9185130756258667E-6, dtssh5ftitw -> 1.9185130756258667E-6, mikhailfranco -> 1.9185130756258667E-6, quotient -> 1.9185130756258667E-6, ...
         */

        Dataset<Row> models = withSLM.select("flair", "statistical_lm", "background_statistical_lm");
        models.printSchema();
        models.show();

        spark.conf().set("spark.sql.crossJoin.enabled", true); // naughty ;)

        // cross join every post with all models
        models.registerTempTable("models");
        data.registerTempTable("data");
        final Dataset<Row> crossJoined = spark.sql(
                "SELECT " +
                            "data.flair AS label, " +
                            "data.created, " +
                            "data.author, " +
                            "data.text, " +
                            "data.words, " +
                            "data.words_freq, " +
                            "models.* " +
                        "FROM data CROSS JOIN models"
        );
        spark.sqlContext().dropTempTable("models");
        crossJoined.printSchema();
        crossJoined.orderBy("author", "created", "flair").show();

        // UDF to calculate score according to
        UDF4 calculateScores = new UDF4<scala.collection.immutable.HashMap<String, Double>, scala.collection.immutable.HashMap<String, Double>, scala.collection.immutable.HashMap<String, Double>, BigDecimal, Double> () {
            public Double call (scala.collection.immutable.HashMap<String, Double> wordFreq, scala.collection.immutable.HashMap<String, Double> statistical_lm, scala.collection.immutable.HashMap<String, Double> background_lm, BigDecimal lambda) {
                assert lambda.doubleValue() >= 0.0 && lambda.doubleValue() <= 1.0;
                final Map<String, Double> wordMap = JavaConversions.asJavaMap(wordFreq);
                Double docSize = wordMap.values().stream().mapToDouble(Double::doubleValue).sum();
                Double alpha = lambda.doubleValue();
                final Map<String, Double> javaStatisticalLM = JavaConversions.asJavaMap(statistical_lm);
                final Map<String, Double> javaBackgroundModel = JavaConversions.asJavaMap(background_lm);

                final Double sum = wordMap.entrySet().parallelStream().map(entry -> {
                    String curWord = entry.getKey();
                    Double cwd = entry.getValue();
                    Double pwd = javaStatisticalLM.getOrDefault(curWord, 0d);
                    Double pwc = javaBackgroundModel.getOrDefault(curWord, 0d);
                    if (pwc != 0) {
                        Double score = (((1.0d - alpha) * pwd) + (alpha * pwc)) / (alpha * pwc);
//                        System.out.println("score: " + score);
                        Double addedend = Math.log(score);
//                        System.out.println("addedend: " + addedend);
                        return addedend;
                    } else return 0.0d;
                }).reduce((a, b) -> {
                    return a + b;
                }).orElse(0.0);

                return sum;
            }
        };
        spark.sqlContext().udf().register("calculateScores", calculateScores, DataTypes.DoubleType);

        crossJoined.registerTempTable("crossJoined");
        final Dataset<Row> withScores = spark.sql(
                "SELECT " +
                        "label, " +
                        "created, " +
                        "author, " +
                        "text, " +
                        "words, " +
                        "words_freq, " +
                        "flair, " +
                        "statistical_lm, " +
                        "background_statistical_lm," +
                        "calculateScores(words_freq, statistical_lm, background_statistical_lm, 0.5) AS score " +
                        "FROM crossJoined"
        );
        spark.sqlContext().dropTempTable("crossJoined");
        withScores.printSchema();
        withScores.orderBy("created", "author", "flair").show();

        /**
         *

         +-----+--------------------+--------------+--------------------+--------------------+-----+--------------------+-------------------------+------------------+
         |label|             created|        author|                text|               words|flair|      statistical_lm|background_statistical_lm|             score|
         +-----+--------------------+--------------+--------------------+--------------------+-----+--------------------+-------------------------+------------------+
         |three|Fri Nov 04 07:40:...|  hammertime89| Cornell Universi...|[, cornell, unive...| null|Map(serious -> 3....|     Map(demsar -> 1.9...| 90.90154703673522|
         |three|Fri Nov 04 07:40:...|  hammertime89| Cornell Universi...|[, cornell, unive...|  two|Map(serious -> 2....|     Map(demsar -> 1.9...| 84.72424611860446|
         |three|Fri Nov 04 07:40:...|  hammertime89| Cornell Universi...|[, cornell, unive...| four|Map(serious -> 2....|     Map(demsar -> 1.9...| 88.04747036542118|
         |three|Fri Nov 04 07:40:...|  hammertime89| Cornell Universi...|[, cornell, unive...|three|Map(mikhailfranco...|     Map(demsar -> 1.9...| 98.54578536339372|
         |three|Fri Nov 04 07:40:...|  hammertime89| Cornell Universi...|[, cornell, unive...|  one|Map(serious -> 2....|     Map(demsar -> 1.9...| 85.26444695593501|
         |three|Fri Nov 04 12:06:...|   schorschico|What do you get i...|[what, do, you, g...| null|Map(serious -> 3....|     Map(demsar -> 1.9...|170.05650644612666|
         |three|Fri Nov 04 12:06:...|   schorschico|What do you get i...|[what, do, you, g...|  two|Map(serious -> 2....|     Map(demsar -> 1.9...|167.30822824110592|
         |three|Fri Nov 04 12:06:...|   schorschico|What do you get i...|[what, do, you, g...|  one|Map(serious -> 2....|     Map(demsar -> 1.9...|171.80117704969126|
         |three|Fri Nov 04 12:06:...|   schorschico|What do you get i...|[what, do, you, g...|three|Map(mikhailfranco...|     Map(demsar -> 1.9...|165.39333249714736|
         |three|Fri Nov 04 12:06:...|   schorschico|What do you get i...|[what, do, you, g...| four|Map(serious -> 2....|     Map(demsar -> 1.9...|164.70882456690825|
         |  two|Fri Nov 04 12:49:...|julian88888888| 3/4 Free Article...|[, 3/4, free, art...|  one|Map(serious -> 2....|     Map(demsar -> 1.9...|307.01418567381796|
         |  two|Fri Nov 04 12:49:...|julian88888888| 3/4 Free Article...|[, 3/4, free, art...|  two|Map(serious -> 2....|     Map(demsar -> 1.9...|  311.653568073366|
         |  two|Fri Nov 04 12:49:...|julian88888888| 3/4 Free Article...|[, 3/4, free, art...| four|Map(serious -> 2....|     Map(demsar -> 1.9...| 305.7219042038655|
         |  two|Fri Nov 04 12:49:...|julian88888888| 3/4 Free Article...|[, 3/4, free, art...| null|Map(serious -> 3....|     Map(demsar -> 1.9...|333.85206362931285|
         |  two|Fri Nov 04 12:49:...|julian88888888| 3/4 Free Article...|[, 3/4, free, art...|three|Map(mikhailfranco...|     Map(demsar -> 1.9...|316.51151044154506|
         |  two|Fri Nov 04 14:48:...|       afeder_| Home Research Pu...|[, home, research...| four|Map(serious -> 2....|     Map(demsar -> 1.9...|239.99699899797253|
         |  two|Fri Nov 04 14:48:...|       afeder_| Home Research Pu...|[, home, research...| null|Map(serious -> 3....|     Map(demsar -> 1.9...|250.58310922197492|
         |  two|Fri Nov 04 14:48:...|       afeder_| Home Research Pu...|[, home, research...|  two|Map(serious -> 2....|     Map(demsar -> 1.9...|251.78605409480156|
         |  two|Fri Nov 04 14:48:...|       afeder_| Home Research Pu...|[, home, research...|three|Map(mikhailfranco...|     Map(demsar -> 1.9...|242.97494508230773|
         |  two|Fri Nov 04 14:48:...|       afeder_| Home Research Pu...|[, home, research...|  one|Map(serious -> 2....|     Map(demsar -> 1.9...|243.05419017767218|
         +-----+--------------------+--------------+--------------------+--------------------+-----+--------------------+-------------------------+------------------+
         */

        final Dataset<Row> toPredict = withScores.select("label", "created", "author", "text", "flair", "score");
        toPredict.printSchema();
        toPredict.orderBy("created", "author", "text", "flair").show();


    }

}
