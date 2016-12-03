//package com.cs410dso.postclassifier;
//
//import com.cs410dso.postclassifier.model.LocalSubredditFlairModel;
//import com.cs410dso.postclassifier.model.SubredditFlairModel;
//
//import org.apache.log4j.Level;
//import org.apache.log4j.LogManager;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.*;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
//import org.apache.spark.ml.feature.*;
//import org.apache.spark.ml.linalg.SparseVector;
//import org.apache.spark.mllib.evaluation.MulticlassMetrics;
//import org.apache.spark.mllib.linalg.Matrix;
//import org.apache.spark.rdd.RDD;
//import org.apache.spark.sql.*;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.api.java.UDF1;
//import org.apache.spark.sql.api.java.UDF3;
//import org.apache.spark.sql.api.java.UDF4;
//import org.apache.spark.sql.api.java.UDF5;
//import org.apache.spark.sql.expressions.WindowSpec;
//import org.apache.spark.sql.types.DataTypes;
//import org.apache.spark.sql.expressions.Window;
//
//import java.io.BufferedOutputStream;
//import java.io.IOException;
//import java.io.OutputStream;
//import java.math.BigDecimal;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.util.*;
//import java.util.function.*;
//import java.util.stream.Stream;
//
//import org.apache.spark.sql.types.StructType;
//import org.codehaus.janino.Java;
//import scala.Array;
//import scala.Function1;
//import scala.Tuple2;
//import scala.collection.Iterable;
//import scala.collection.JavaConversions;
//import scala.collection.Seq;
//
//import org.apache.spark.sql.SQLImplicits.*;
//
//import static java.nio.file.StandardOpenOption.APPEND;
//import static java.nio.file.StandardOpenOption.CREATE;
//import static scala.collection.JavaConversions.*;
//
///**
// * App main
// */
//public class AppSpark {
//
//    public static void main(String[] args) {
//
//        // Spark setup
//        SparkSession spark = SparkSession.builder().appName("Reddit Post Classifier").master("local[4]").getOrCreate();
//        // https://stackoverflow.com/questions/31951728/how-to-set-up-logging-level-for-spark-application-in-intellij-idea
//        LogManager.getRootLogger().setLevel(Level.ERROR); // hide INFO
//
//        // scrape and in¡gest
//        ArrayList<String> listOfSubreddits = new ArrayList();
//        listOfSubreddits.add("machinelearning");
//        SubredditFlairModel subredditFlairModel = new SubredditFlairModel(spark, listOfSubreddits, 1000);
////        LocalSubredditFlairModel subredditFlairModel = new LocalSubredditFlairModel(spark); // change to this if behind corporate firewall and you have data.json
//        Dataset<Row> dataRawWithNull = subredditFlairModel.getProcessedWords();
//        Dataset<Row> dataRawWithoutIndexedFlairs = dataRawWithNull.where(dataRawWithNull.col("flair").isNotNull()); // filter out null flairs or flairs that don't have css
//
//        StringIndexerModel flairStringIndexerModel = new StringIndexer().setInputCol("flair").setOutputCol("indexed_flair").fit(dataRawWithoutIndexedFlairs);
//        final Dataset<Row> withIndexedFlair = flairStringIndexerModel.transform(dataRawWithoutIndexedFlairs);
//
//        withIndexedFlair.printSchema();
//        withIndexedFlair.show();
//
//        /*
//         root
//         |-- author: string (nullable = true)
//         |-- created: string (nullable = true)
//         |-- flair: string (nullable = true)
//         |-- text: string (nullable = true)
//         |-- words: array (nullable = true)
//         |    |-- element: string (containsNull = true)
//         |-- indexed_flair: double (nullable = true)
//
//         +------------------+--------------------+-----+--------------------+--------------------+-------------+
//         |            author|             created|flair|                text|               words|indexed_flair|
//         +------------------+--------------------+-----+--------------------+--------------------+-------------+
//         |       cjmcmurtrie|Tue Nov 01 09:52:...|  one|I have implemente...|[i, have, impleme...|          1.0|
//         |         c_y_smith|Tue Oct 11 17:30:...| four|This is a TensorF...|[this, is, a, ten...|          2.0|
//         |   belsnickel4ever|Sat Nov 26 02:15:...|  one|Hi, anyone intere...|[hi,, anyone, int...|          1.0|
//         |      perceptron01|Sat Nov 12 18:47:...|  one|Couldn't find any...|[couldn't, find, ...|          1.0|
//         |           rcmalli|Tue Oct 18 19:08:...| four| Skip to content ...|[, skip, to, cont...|          2.0|
//         |          drlukeor|Wed Nov 30 00:09:...|  one|If you ask academ...|[if, you, ask, ac...|          1.0|
//         |          sybilckw|Mon Oct 17 09:39:...|three|DataGenCARS is a ...|[datagencars, is,...|          0.0|
//         |      downtownslim|Fri Oct 14 20:08:...|  two|Some of the lates...|[some, of, the, l...|          3.0|
//         |          sybilckw|Mon Nov 14 08:20:...|three| Cornell Universi...|[, cornell, unive...|          0.0|
//         |          sybilckw|Thu Nov 10 06:20:...|three| Cornell Universi...|[, cornell, unive...|          0.0|
//         |      morgangiraud|Wed Nov 09 08:01:...| four|This repo is the ...|[this, repo, is, ...|          2.0|
//         |     paulhendricks|Tue Oct 25 10:10:...| four| Skip to content ...|[, skip, to, cont...|          2.0|
//         |      PachecoAndre|Sun Oct 30 12:54:...|three| Ranking of class...|[, ranking, of, c...|          0.0|
//         |           egrefen|Wed Oct 12 13:59:...|three| Home Research Pu...|[, home, research...|          0.0|
//         |       ill-logical|Tue Oct 11 14:14:...|  one|It used to be tha...|[it, used, to, be...|          1.0|
//         |The_Man_of_Science|Wed Nov 16 22:54:...|three| Cornell Universi...|[, cornell, unive...|          0.0|
//         |        Mandrathax|Mon Nov 28 10:16:...|  one|This is a place t...|[this, is, a, pla...|          1.0|
//         |       Thenewcheri|Tue Nov 08 07:35:...|three|I have been learn...|[i, have, been, l...|          0.0|
//         |             lioru|Mon Nov 07 05:03:...|  one|Hi all,
//
//         I read ...|[hi, all,, , , i,...|          1.0|
//         |          hoqqanen|Fri Oct 07 20:33:...|three| Cornell Universi...|[, cornell, unive...|          0.0|
//         +------------------+--------------------+-----+--------------------+--------------------+-------------+
//         only showing top 20 rows
//         */
//
//        System.out.println("number of entries: " + Long.toString(withIndexedFlair.count()));
//
//        final CountVectorizerModel dataCVModel = new CountVectorizer().setInputCol("words").setOutputCol("words_features").fit(withIndexedFlair);
//        final Dataset<Row> withWordsFeatures = dataCVModel.transform(withIndexedFlair);
//        withWordsFeatures.printSchema();
//        withWordsFeatures.show();
//
//        /*
//         root
//         |-- author: string (nullable = true)
//         |-- created: string (nullable = true)
//         |-- flair: string (nullable = true)
//         |-- text: string (nullable = true)
//         |-- words: array (nullable = true)
//         |    |-- element: string (containsNull = true)
//         |-- indexed_flair: double (nullable = true)
//         |-- words_features: vector (nullable = true)
//
//         +------------------+--------------------+-----+--------------------+--------------------+-------------+--------------------+
//         |            author|             created|flair|                text|               words|indexed_flair|      words_features|
//         +------------------+--------------------+-----+--------------------+--------------------+-------------+--------------------+
//         |       cjmcmurtrie|Tue Nov 01 09:52:...|  one|I have implemente...|[i, have, impleme...|          1.0|(44553,[1,2,3,4,6...|
//         |         c_y_smith|Tue Oct 11 17:30:...| four|This is a TensorF...|[this, is, a, ten...|          2.0|(44553,[0,1,2,3,4...|
//         |   belsnickel4ever|Sat Nov 26 02:15:...|  one|Hi, anyone intere...|[hi,, anyone, int...|          1.0|(44553,[0,1,2,3,4...|
//         |      perceptron01|Sat Nov 12 18:47:...|  one|Couldn't find any...|[couldn't, find, ...|          1.0|(44553,[0,1,5,7,9...|
//         |           rcmalli|Tue Oct 18 19:08:...| four| Skip to content ...|[, skip, to, cont...|          2.0|(44553,[0,2,4,5,6...|
//         |          drlukeor|Wed Nov 30 00:09:...|  one|If you ask academ...|[if, you, ask, ac...|          1.0|(44553,[0,1,2,3,4...|
//         |          sybilckw|Mon Oct 17 09:39:...|three|DataGenCARS is a ...|[datagencars, is,...|          0.0|(44553,[0,1,2,3,4...|
//         |      downtownslim|Fri Oct 14 20:08:...|  two|Some of the lates...|[some, of, the, l...|          3.0|(44553,[0,1,2,3,4...|
//         |          sybilckw|Mon Nov 14 08:20:...|three| Cornell Universi...|[, cornell, unive...|          0.0|(44553,[0,1,2,3,4...|
//         |          sybilckw|Thu Nov 10 06:20:...|three| Cornell Universi...|[, cornell, unive...|          0.0|(44553,[0,1,2,4,5...|
//         |      morgangiraud|Wed Nov 09 08:01:...| four|This repo is the ...|[this, repo, is, ...|          2.0|(44553,[0,1,2,3,4...|
//         |     paulhendricks|Tue Oct 25 10:10:...| four| Skip to content ...|[, skip, to, cont...|          2.0|(44553,[0,1,2,3,4...|
//         |      PachecoAndre|Sun Oct 30 12:54:...|three| Ranking of class...|[, ranking, of, c...|          0.0|(44553,[0,1,2,3,4...|
//         |           egrefen|Wed Oct 12 13:59:...|three| Home Research Pu...|[, home, research...|          0.0|(44553,[0,1,2,3,4...|
//         |       ill-logical|Tue Oct 11 14:14:...|  one|It used to be tha...|[it, used, to, be...|          1.0|(44553,[0,1,2,3,4...|
//         |The_Man_of_Science|Wed Nov 16 22:54:...|three| Cornell Universi...|[, cornell, unive...|          0.0|(44553,[0,1,2,3,4...|
//         |        Mandrathax|Mon Nov 28 10:16:...|  one|This is a place t...|[this, is, a, pla...|          1.0|(44553,[0,1,2,3,4...|
//         |       Thenewcheri|Tue Nov 08 07:35:...|three|I have been learn...|[i, have, been, l...|          0.0|(44553,[0,1,2,3,4...|
//         |             lioru|Mon Nov 07 05:03:...|  one|Hi all,
//
//         I read ...|[hi, all,, , , i,...|          1.0|(44553,[0,2,5,7,8...|
//         |          hoqqanen|Fri Oct 07 20:33:...|three| Cornell Universi...|[, cornell, unive...|          0.0|(44553,[0,1,2,3,4...|
//         +------------------+--------------------+-----+--------------------+--------------------+-------------+--------------------+
//         only showing top 20 rows
//         */
//
//        // the UDF for word counts
//        final String[] postVocab = dataCVModel.vocabulary();
//        UDF1 postWordFreqFromCountVectorizerModel = new UDF1<SparseVector, Map<String, Double>>() {
//            public Map<String, Double> call(SparseVector v) {
//                SparseVector sv = v.toSparse();
//                int length = sv.indices().length;
//                final double[] values = sv.values();
//                Map<String, Double> myMap = new HashMap<>();
//                for(int i = 0; i < length; i++) {
//                    myMap.put(postVocab[i], values[i]);
//                }
//                return myMap;
//            }
//        };
//        spark.sqlContext().udf().register("postWordFreqFromCountVectorizerModel", postWordFreqFromCountVectorizerModel, DataTypes.createMapType(DataTypes.StringType, DataTypes.DoubleType));
//
//        // the SQL query for word counts
//        withWordsFeatures.registerTempTable("withWordsFeatures");
//        final Dataset<Row> data = spark.sql(
//                "SELECT " +
//                        "author, " +
//                        "created, " +
//                        "indexed_flair, " +
//                        "text, " +
//                        "words, " +
//                        "words_features, " +
//                        "postWordFreqFromCountVectorizerModel(words_features) AS words_freq " +
//                        "FROM withWordsFeatures"
//        );
//        spark.sqlContext().dropTempTable("withWordsFeatures");
//        // resultant word count
//        data.printSchema();
//        data.show();
//
//        /*
//         root
//         |-- author: string (nullable = true)
//         |-- created: string (nullable = true)
//         |-- indexed_flair: double (nullable = true)
//         |-- text: string (nullable = true)
//         |-- words: array (nullable = true)
//         |    |-- element: string (containsNull = true)
//         |-- words_features: vector (nullable = true)
//         |-- words_freq: map (nullable = true)
//         |    |-- key: string
//         |    |-- value: double (valueContainsNull = true)
//
//         +------------------+--------------------+-------------+--------------------+--------------------+--------------------+--------------------+
//         |            author|             created|indexed_flair|                text|               words|      words_features|          words_freq|
//         +------------------+--------------------+-------------+--------------------+--------------------+--------------------+--------------------+
//         |       cjmcmurtrie|Tue Nov 01 09:52:...|          1.0|I have implemente...|[i, have, impleme...|(44553,[1,2,3,4,6...|Map(used -> 1.0, ...|
//         |         c_y_smith|Tue Oct 11 17:30:...|          2.0|This is a TensorF...|[this, is, a, ten...|(44553,[0,1,2,3,4...|Map(rate -> 1.0, ...|
//         |   belsnickel4ever|Sat Nov 26 02:15:...|          1.0|Hi, anyone intere...|[hi,, anyone, int...|(44553,[0,1,2,3,4...|Map(used -> 1.0, ...|
//         |      perceptron01|Sat Nov 12 18:47:...|          1.0|Couldn't find any...|[couldn't, find, ...|(44553,[0,1,5,7,9...|Map( -> 1.0, for ...|
//         |           rcmalli|Tue Oct 18 19:08:...|          2.0| Skip to content ...|[, skip, to, cont...|(44553,[0,2,4,5,6...|Map(latent -> 1.0...|
//         |          drlukeor|Wed Nov 30 00:09:...|          1.0|If you ask academ...|[if, you, ask, ac...|(44553,[0,1,2,3,4...|Map(rate -> 2.0, ...|
//         |          sybilckw|Mon Oct 17 09:39:...|          0.0|DataGenCARS is a ...|[datagencars, is,...|(44553,[0,1,2,3,4...|Map(used -> 1.0, ...|
//         |      downtownslim|Fri Oct 14 20:08:...|          3.0|Some of the lates...|[some, of, the, l...|(44553,[0,1,2,3,4...|Map(al., -> 1.0, ...|
//         |          sybilckw|Mon Nov 14 08:20:...|          0.0| Cornell Universi...|[, cornell, unive...|(44553,[0,1,2,3,4...|Map(al., -> 1.0, ...|
//         |          sybilckw|Thu Nov 10 06:20:...|          0.0| Cornell Universi...|[, cornell, unive...|(44553,[0,1,2,4,5...|Map(al., -> 1.0, ...|
//         |      morgangiraud|Wed Nov 09 08:01:...|          2.0|This repo is the ...|[this, repo, is, ...|(44553,[0,1,2,3,4...|Map(al., -> 1.0, ...|
//         |     paulhendricks|Tue Oct 25 10:10:...|          2.0| Skip to content ...|[, skip, to, cont...|(44553,[0,1,2,3,4...|Map(latent -> 1.0...|
//         |      PachecoAndre|Sun Oct 30 12:54:...|          0.0| Ranking of class...|[, ranking, of, c...|(44553,[0,1,2,3,4...|Map(rate -> 4.0, ...|
//         |           egrefen|Wed Oct 12 13:59:...|          0.0| Home Research Pu...|[, home, research...|(44553,[0,1,2,3,4...|Map(rate -> 1.0, ...|
//         |       ill-logical|Tue Oct 11 14:14:...|          1.0|It used to be tha...|[it, used, to, be...|(44553,[0,1,2,3,4...|Map(used -> 1.0, ...|
//         |The_Man_of_Science|Wed Nov 16 22:54:...|          0.0| Cornell Universi...|[, cornell, unive...|(44553,[0,1,2,3,4...|Map(al., -> 1.0, ...|
//         |        Mandrathax|Mon Nov 28 10:16:...|          1.0|This is a place t...|[this, is, a, pla...|(44553,[0,1,2,3,4...|Map(al., -> 1.0, ...|
//         |       Thenewcheri|Tue Nov 08 07:35:...|          0.0|I have been learn...|[i, have, been, l...|(44553,[0,1,2,3,4...|Map(used -> 1.0, ...|
//         |             lioru|Mon Nov 07 05:03:...|          1.0|Hi all,
//
//         I read ...|[hi, all,, , , i,...|(44553,[0,2,5,7,8...|Map( -> 1.0, for ...|
//         |          hoqqanen|Fri Oct 07 20:33:...|          0.0| Cornell Universi...|[, cornell, unive...|(44553,[0,1,2,3,4...|Map(al., -> 1.0, ...|
//         +------------------+--------------------+-------------+--------------------+--------------------+--------------------+--------------------+
//         only showing top 20 rows
//
//         */
//
//        // what flairs do we have?
//        Dataset<Row> flairsDS = withIndexedFlair.select("indexed_flair").dropDuplicates();
//        List<String> flairs = flairsDS.toJavaRDD().map(new Function<Row, String>() {
//            public String call(Row row) {
//                return row.toString();
//            }
//        }).collect();
//        flairs.forEach(f -> System.out.println(f));
//
//        /*
//         [0.0]
//         [1.0]
//         [3.0]
//         [2.0]
//         */
//
//        // calculate the priors
//        final List<Row> indexed_flairs = withIndexedFlair.select("indexed_flair").toJavaRDD().collect();
//        Map<Double, Double> flairCounts = new HashMap<Double, Double>();
//        indexed_flairs.stream().forEach(row -> {
//            java.lang.Double curFlair = row.getDouble(0);
//            java.lang.Double currentCount = flairCounts.getOrDefault(curFlair,0.0);
//            flairCounts.put(curFlair, currentCount + 1);
//        });
//        final double sumFlairs = flairCounts.values().stream().mapToDouble(Double::doubleValue).sum();
//        Map<Double, Double> priors = new HashMap<Double, Double>();
//        flairCounts.entrySet().forEach(entry -> {
//            Double curFlair = entry.getKey();
//            Double prior = entry.getValue() / sumFlairs;
//            priors.put(curFlair, prior);
//        });
//
//        // for each flair, get the concatenated text from all posts
//        // https://stackoverflow.com/questions/34150547/spark-group-concat-equivalent-in-scala-rdd
//        // https://spark.apache.org/docs/2.0.2/api/java/org/apache/spark/sql/functions.html#concat_ws(java.lang.String,%20org.apache.spark.sql.Column...)
//        withIndexedFlair.registerTempTable("dataRawWithoutIndexedFlairs");
//        final Dataset<Row> flairAndConcatText = spark.sql(
//                "SELECT " +
//                        "indexed_flair, " +
//                        "concat_ws( ' ', collect_list(text)) AS concat_text " +
//                        "FROM dataRawWithoutIndexedFlairs " +
//                        "GROUP BY indexed_flair");
//        spark.sqlContext().dropTempTable("dataRawWithoutIndexedFlairs");
//        flairAndConcatText.show();
//
//        /*
//         +-------------+--------------------+
//         |indexed_flair|         concat_text|
//         +-------------+--------------------+
//         |          0.0|DataGenCARS is a ...|
//         |          1.0|I have implemente...|
//         |          3.0|Some of the lates...|
//         |          2.0|This is a TensorF...|
//         +-------------+--------------------+
//         */
//
//        // get the combined text of all posts
//        final String allPostsText = flairAndConcatText.select("concat_text").toJavaRDD().map(new Function<Row, String>() {
//            public String call(Row row) {
//                return row.get(0).toString();
//            }
//        }).reduce(new Function2<String, String, String>() {
//            public String call(String s1, String s2) {
//                return s1 + " " + s2;
//            }
//        });
//
//        // add background text as column
//        final Dataset<Row> withBackgroundText = flairAndConcatText.withColumn("background_text", functions.lit(allPostsText));
//        withBackgroundText.show();
//
//        /*
//         +-------------+--------------------+--------------------+
//         |indexed_flair|         concat_text|     background_text|
//         +-------------+--------------------+--------------------+
//         |          0.0|DataGenCARS is a ...|Some of the lates...|
//         |          1.0|I have implemente...|Some of the lates...|
//         |          3.0|Some of the lates...|Some of the lates...|
//         |          2.0|This is a TensorF...|Some of the lates...|
//         +-------------+--------------------+--------------------+
//         */
//
//        // get the background words out
//        RegexTokenizer backgroundTokenizer = new RegexTokenizer().setInputCol("background_text").setOutputCol("background_words").setPattern("\\W");
//        final Dataset<Row> withBackgroundWords = backgroundTokenizer.transform(withBackgroundText);
//        withBackgroundWords.printSchema();
//        withBackgroundWords.show();
//
//        /*
//         root
//         |-- indexed_flair: double (nullable = true)
//         |-- concat_text: string (nullable = false)
//         |-- background_text: string (nullable = false)
//         |-- background_words: array (nullable = true)
//         |    |-- element: string (containsNull = true)
//
//         +-------------+--------------------+--------------------+--------------------+
//         |indexed_flair|         concat_text|     background_text|    background_words|
//         +-------------+--------------------+--------------------+--------------------+
//         |          0.0|DataGenCARS is a ...|Some of the lates...|[some, of, the, l...|
//         |          1.0|I have implemente...|Some of the lates...|[some, of, the, l...|
//         |          3.0|Some of the lates...|Some of the lates...|[some, of, the, l...|
//         |          2.0|This is a TensorF...|Some of the lates...|[some, of, the, l...|
//         +-------------+--------------------+--------------------+--------------------+
//
//         */
//
//        // get the words out
//        // https://spark.apache.org/docs/latest/ml-features.html#tokenizer
//        // `\\W` pattern is a nonword character: [^A-Za-z0-9_] (see https://www.tutorialspoint.com/scala/scala_regular_expressions.htm)
//        // this transform also forces lower case
//        RegexTokenizer tokenizer = new RegexTokenizer().setInputCol("concat_text").setOutputCol("words").setPattern("\\W");
//        final Dataset<Row> flairAndWords = tokenizer.transform(withBackgroundWords);
//
//        flairAndWords.printSchema();
//        flairAndWords.show();
//        /*
//        root
//         |-- indexed_flair: double (nullable = true)
//         |-- concat_text: string (nullable = false)
//         |-- background_text: string (nullable = false)
//         |-- background_words: array (nullable = true)
//         |    |-- element: string (containsNull = true)
//         |-- words: array (nullable = true)
//         |    |-- element: string (containsNull = true)
//
//        +-------------+--------------------+--------------------+--------------------+--------------------+
//        |indexed_flair|         concat_text|     background_text|    background_words|               words|
//        +-------------+--------------------+--------------------+--------------------+--------------------+
//        |          0.0|DataGenCARS is a ...|Some of the lates...|[some, of, the, l...|[datagencars, is,...|
//        |          1.0|I have implemente...|Some of the lates...|[some, of, the, l...|[i, have, impleme...|
//        |          3.0|Some of the lates...|Some of the lates...|[some, of, the, l...|[some, of, the, l...|
//        |          2.0|This is a TensorF...|Some of the lates...|[some, of, the, l...|[this, is, a, ten...|
//        +-------------+--------------------+--------------------+--------------------+--------------------+
//         */
//
//        final CountVectorizerModel backgroundCVModel = new CountVectorizer().setInputCol("background_words").setOutputCol("background_features").fit(flairAndWords);
//        final Dataset<Row> withBackgroundFeatures = backgroundCVModel.transform(flairAndWords);
//        withBackgroundFeatures.printSchema();
//        withBackgroundFeatures.show();
//
//        /*
//        root
//         |-- indexed_flair: double (nullable = true)
//         |-- concat_text: string (nullable = false)
//         |-- background_text: string (nullable = false)
//         |-- background_words: array (nullable = true)
//         |    |-- element: string (containsNull = true)
//         |-- words: array (nullable = true)
//         |    |-- element: string (containsNull = true)
//         |-- background_features: vector (nullable = true)
//
//        +-------------+--------------------+--------------------+--------------------+--------------------+--------------------+
//        |indexed_flair|         concat_text|     background_text|    background_words|               words| background_features|
//        +-------------+--------------------+--------------------+--------------------+--------------------+--------------------+
//        |          0.0|DataGenCARS is a ...|Some of the lates...|[some, of, the, l...|[datagencars, is,...|(21958,[0,1,2,3,4...|
//        |          1.0|I have implemente...|Some of the lates...|[some, of, the, l...|[i, have, impleme...|(21958,[0,1,2,3,4...|
//        |          3.0|Some of the lates...|Some of the lates...|[some, of, the, l...|[some, of, the, l...|(21958,[0,1,2,3,4...|
//        |          2.0|This is a TensorF...|Some of the lates...|[some, of, the, l...|[this, is, a, ten...|(21958,[0,1,2,3,4...|
//        +-------------+--------------------+--------------------+--------------------+--------------------+--------------------+
//         */
//
//        // https://stackoverflow.com/questions/34423281/spark-dataframe-word-count-per-document-single-row-per-document
//        final CountVectorizerModel cvModel = new CountVectorizer().setInputCol("words").setOutputCol("features").fit(withBackgroundFeatures);
//        final Dataset<Row> counted = cvModel.transform(withBackgroundFeatures);
//        counted.printSchema();
//        counted.show();
//
//        /*
//        root
//         |-- indexed_flair: double (nullable = true)
//         |-- concat_text: string (nullable = false)
//         |-- background_text: string (nullable = false)
//         |-- background_words: array (nullable = true)
//         |    |-- element: string (containsNull = true)
//         |-- words: array (nullable = true)
//         |    |-- element: string (containsNull = true)
//         |-- background_features: vector (nullable = true)
//         |-- features: vector (nullable = true)
//
//        +-------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
//        |indexed_flair|         concat_text|     background_text|    background_words|               words| background_features|            features|
//        +-------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
//        |          0.0|DataGenCARS is a ...|Some of the lates...|[some, of, the, l...|[datagencars, is,...|(21958,[0,1,2,3,4...|(21958,[0,1,2,3,4...|
//        |          1.0|I have implemente...|Some of the lates...|[some, of, the, l...|[i, have, impleme...|(21958,[0,1,2,3,4...|(21958,[0,1,2,3,4...|
//        |          3.0|Some of the lates...|Some of the lates...|[some, of, the, l...|[some, of, the, l...|(21958,[0,1,2,3,4...|(21958,[0,1,2,3,4...|
//        |          2.0|This is a TensorF...|Some of the lates...|[some, of, the, l...|[this, is, a, ten...|(21958,[0,1,2,3,4...|(21958,[0,1,2,3,4...|
//        +-------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
//         */
//
//        // the UDF for word counts
//        final String[] vocabulary = cvModel.vocabulary();
//        UDF1 wordFreqFromCountVectorizerModel = new UDF1<SparseVector, Map<String, Double>>() {
//            public Map<String, Double> call(SparseVector v) {
//                SparseVector sv = v.toSparse();
//                int length = sv.indices().length;
//                final double[] values = sv.values();
//                Map<String, Double> myMap = new HashMap<>();
//                for(int i = 0; i < length; i++) {
//                    myMap.put(vocabulary[i], values[i]);
//                }
//                return myMap;
//            }
//        };
//        spark.sqlContext().udf().register("wordFreqFromCountVectorizerModel", wordFreqFromCountVectorizerModel, DataTypes.createMapType(DataTypes.StringType, DataTypes.DoubleType));
//
//        // debugging
//        int vocabLength = cvModel.vocabulary().length;
//        System.out.println("vocab length: " + vocabLength);
//
//        // the SQL query for word counts
//        counted.registerTempTable("counted");
//        final Dataset<Row> withFreq = spark.sql(
//                "SELECT " +
//                        "indexed_flair, " +
//                        "concat_text, " +
//                        "words, " +
//                        "features, " +
//                        "wordFreqFromCountVectorizerModel(features) AS model_freq, " +
//                        "background_text, " +
//                        "background_words, " +
//                        "background_features, " +
//                        "wordFreqFromCountVectorizerModel(background_features) AS background_freq " +
//                        "FROM counted "
//        );
//        spark.sqlContext().dropTempTable("counted");
//        // resultant word count
//        withFreq.printSchema();
//        withFreq.show();
//
//        /*
//        root
//         |-- indexed_flair: double (nullable = true)
//         |-- concat_text: string (nullable = false)
//         |-- words: array (nullable = true)
//         |    |-- element: string (containsNull = true)
//         |-- features: vector (nullable = true)
//         |-- model_freq: map (nullable = true)
//         |    |-- key: string
//         |    |-- value: double (valueContainsNull = true)
//         |-- background_text: string (nullable = false)
//         |-- background_words: array (nullable = true)
//         |    |-- element: string (containsNull = true)
//         |-- background_features: vector (nullable = true)
//         |-- background_freq: map (nullable = true)
//         |    |-- key: string
//         |    |-- value: double (valueContainsNull = true)
//
//        +-------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
//        |indexed_flair|         concat_text|               words|            features|          model_freq|     background_text|    background_words| background_features|     background_freq|
//        +-------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
//        |          0.0|DataGenCARS is a ...|[datagencars, is,...|(21958,[0,1,2,3,4...|Map(serious -> 1....|Some of the lates...|[some, of, the, l...|(21958,[0,1,2,3,4...|Map(demsar -> 1.0...|
//        |          1.0|I have implemente...|[i, have, impleme...|(21958,[0,1,2,3,4...|Map(gans -> 7.0, ...|Some of the lates...|[some, of, the, l...|(21958,[0,1,2,3,4...|Map(demsar -> 1.0...|
//        |          3.0|Some of the lates...|[some, of, the, l...|(21958,[0,1,2,3,4...|Map(gans -> 5.0, ...|Some of the lates...|[some, of, the, l...|(21958,[0,1,2,3,4...|Map(demsar -> 1.0...|
//        |          2.0|This is a TensorF...|[this, is, a, ten...|(21958,[0,1,2,3,4...|Map(serious -> 1....|Some of the lates...|[some, of, the, l...|(21958,[0,1,2,3,4...|Map(demsar -> 1.0...|
//        +-------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
//         */
//
//        // UDF for statistical language model
//        UDF1 statisicalLMFromWordFreq = new UDF1<scala.collection.immutable.HashMap<String, Double>, scala.collection.mutable.HashMap<String, Double>>() {
//            public scala.collection.mutable.HashMap<String, Double> call(scala.collection.immutable.HashMap<String, Double> wordFreq) {
//                List<Double> valuesList = asJavaList(wordFreq.values().toList());
//                Double sum = valuesList.stream().mapToDouble(Double::doubleValue).sum();
//                scala.collection.mutable.HashMap<String, Double> myMap = new scala.collection.mutable.HashMap<String, Double>();
//                final Map<String, Double> javaMap = mapAsJavaMap(wordFreq);
//                javaMap.keySet().forEach(word -> {
//                    Double freq = javaMap.get(word);
//                    Double newFreq = freq / sum; // the main logic
//                    myMap.put(word, newFreq);
//                });
//                return myMap;
//            }
//        };
//
//        // the SQL query for word counts
//        withFreq.registerTempTable("withFreq");
//        spark.sqlContext().udf().register("statisicalLMFromWordFreq", statisicalLMFromWordFreq, DataTypes.createMapType(DataTypes.StringType, DataTypes.DoubleType));
//        final Dataset<Row> withSLM = spark.sql(
//                "SELECT " +
//                        "indexed_flair, " +
//                        "concat_text, " +
//                        "words, " +
//                        "features, " +
//                        "model_freq, " +
//                        "statisicalLMFromWordFreq(model_freq) AS statistical_lm, " +
//                        "background_text, " +
//                        "background_words, " +
//                        "background_features, " +
//                        "background_freq, " +
//                        "statisicalLMFromWordFreq(background_freq) AS background_statistical_lm " +
//                        "FROM withFreq "
//        );
//        spark.sqlContext().dropTempTable("withFreq");
//        // resultant word count
//        withSLM.printSchema();
//        withSLM.show();
//
//        /*
//        root
//         |-- indexed_flair: double (nullable = true)
//         |-- concat_text: string (nullable = false)
//         |-- words: array (nullable = true)
//         |    |-- element: string (containsNull = true)
//         |-- features: vector (nullable = true)
//         |-- model_freq: map (nullable = true)
//         |    |-- key: string
//         |    |-- value: double (valueContainsNull = true)
//         |-- statistical_lm: map (nullable = true)
//         |    |-- key: string
//         |    |-- value: double (valueContainsNull = true)
//         |-- background_text: string (nullable = false)
//         |-- background_words: array (nullable = true)
//         |    |-- element: string (containsNull = true)
//         |-- background_features: vector (nullable = true)
//         |-- background_freq: map (nullable = true)
//         |    |-- key: string
//         |    |-- value: double (valueContainsNull = true)
//         |-- background_statistical_lm: map (nullable = true)
//         |    |-- key: string
//         |    |-- value: double (valueContainsNull = true)
//
//        +-------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+-------------------------+
//        |indexed_flair|         concat_text|               words|            features|          model_freq|      statistical_lm|     background_text|    background_words| background_features|     background_freq|background_statistical_lm|
//        +-------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+-------------------------+
//        |          0.0|DataGenCARS is a ...|[datagencars, is,...|(21958,[0,1,2,3,4...|Map(serious -> 1....|Map(serious -> 3....|Some of the lates...|[some, of, the, l...|(21958,[0,1,2,3,4...|Map(demsar -> 1.0...|     Map(demsar -> 2.3...|
//        |          1.0|I have implemente...|[i, have, impleme...|(21958,[0,1,2,3,4...|Map(gans -> 7.0, ...|Map(gans -> 1.727...|Some of the lates...|[some, of, the, l...|(21958,[0,1,2,3,4...|Map(demsar -> 1.0...|     Map(demsar -> 2.3...|
//        |          3.0|Some of the lates...|[some, of, the, l...|(21958,[0,1,2,3,4...|Map(gans -> 5.0, ...|Map(gans -> 1.294...|Some of the lates...|[some, of, the, l...|(21958,[0,1,2,3,4...|Map(demsar -> 1.0...|     Map(demsar -> 2.3...|
//        |          2.0|This is a TensorF...|[this, is, a, ten...|(21958,[0,1,2,3,4...|Map(serious -> 1....|Map(serious -> 1....|Some of the lates...|[some, of, the, l...|(21958,[0,1,2,3,4...|Map(demsar -> 1.0...|     Map(demsar -> 2.3...|
//        +-------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+-------------------------+
//
//         */
//
//        // ta-dah!
//        Iterator<Row> rowIterator = withSLM.toJavaRDD().toLocalIterator();
//        Row row = rowIterator.next();
//        System.out.println("indexed_flair: " + row.get(0).toString());
//        System.out.println("statistical_lm: " + row.get(5).toString().substring(0,100));
//        System.out.println("background_statistical_lm: " + row.get(10).toString().substring(0,100));
//
//        /*
//        indexed_flair: 0.0
//        statistical_lm: Map(serious -> 3.636972002589524E-6, gans -> 2.0367043214501336E-4, regularizing -> 3.63697200258952
//        background_statistical_lm: Map(demsar -> 2.324105858373637E-6, dtssh5ftitw -> 2.324105858373637E-6, quotient -> 2.3241058583736
//         */
//
//        Dataset<Row> models = withSLM.select("indexed_flair", "statistical_lm", "background_statistical_lm");
//        models.printSchema();
//        models.show();
//
//        /*
//        root
//         |-- indexed_flair: double (nullable = true)
//         |-- statistical_lm: map (nullable = true)
//         |    |-- key: string
//         |    |-- value: double (valueContainsNull = true)
//         |-- background_statistical_lm: map (nullable = true)
//         |    |-- key: string
//         |    |-- value: double (valueContainsNull = true)
//
//        +-------------+--------------------+-------------------------+
//        |indexed_flair|      statistical_lm|background_statistical_lm|
//        +-------------+--------------------+-------------------------+
//        |          0.0|Map(serious -> 3....|     Map(demsar -> 2.3...|
//        |          1.0|Map(gans -> 1.727...|     Map(demsar -> 2.3...|
//        |          3.0|Map(gans -> 1.294...|     Map(demsar -> 2.3...|
//        |          2.0|Map(serious -> 1....|     Map(demsar -> 2.3...|
//        +-------------+--------------------+-------------------------+
//         */
//
//        spark.conf().set("spark.sql.crossJoin.enabled", true); // naughty ;)
//
//        // cross join every post with all models
//        models.registerTempTable("models");
//        data.registerTempTable("data");
//        final Dataset<Row> crossJoined = spark.sql(
//                "SELECT " +
//                            "data.indexed_flair AS label, " +
//                            "data.created, " +
//                            "data.author, " +
//                            "data.text, " +
//                            "data.words, " +
//                            "data.words_freq, " +
//                            "models.* " +
//                        "FROM data CROSS JOIN models"
//        );
//        spark.sqlContext().dropTempTable("models");
//        crossJoined.printSchema();
//        crossJoined.orderBy("author", "created", "indexed_flair").show();
//
//        /*
//        root
//         |-- label: double (nullable = true)
//         |-- created: string (nullable = true)
//         |-- author: string (nullable = true)
//         |-- text: string (nullable = true)
//         |-- words: array (nullable = true)
//         |    |-- element: string (containsNull = true)
//         |-- words_freq: map (nullable = true)
//         |    |-- key: string
//         |    |-- value: double (valueContainsNull = true)
//         |-- indexed_flair: double (nullable = true)
//         |-- statistical_lm: map (nullable = true)
//         |    |-- key: string
//         |    |-- value: double (valueContainsNull = true)
//         |-- background_statistical_lm: map (nullable = true)
//         |    |-- key: string
//         |    |-- value: double (valueContainsNull = true)
//
//        +-----+--------------------+-----------+--------------------+--------------------+--------------------+-------------+--------------------+-------------------------+
//        |label|             created|     author|                text|               words|          words_freq|indexed_flair|      statistical_lm|background_statistical_lm|
//        +-----+--------------------+-----------+--------------------+--------------------+--------------------+-------------+--------------------+-------------------------+
//        |  2.0|Mon Nov 07 04:46:...|3eyedravens|This work was car...|[this, work, was,...|Map(al., -> 1.0, ...|          0.0|Map(serious -> 3....|     Map(demsar -> 2.3...|
//        |  2.0|Mon Nov 07 04:46:...|3eyedravens|This work was car...|[this, work, was,...|Map(al., -> 1.0, ...|          1.0|Map(gans -> 1.727...|     Map(demsar -> 2.3...|
//        |  2.0|Mon Nov 07 04:46:...|3eyedravens|This work was car...|[this, work, was,...|Map(al., -> 1.0, ...|          2.0|Map(serious -> 1....|     Map(demsar -> 2.3...|
//        |  2.0|Mon Nov 07 04:46:...|3eyedravens|This work was car...|[this, work, was,...|Map(al., -> 1.0, ...|          3.0|Map(gans -> 1.294...|     Map(demsar -> 2.3...|
//        |  1.0|Wed Nov 09 14:12:...|   AnvaMiba|I'm looking for d...|[i'm, looking, fo...|Map(used -> 1.0, ...|          0.0|Map(serious -> 3....|     Map(demsar -> 2.3...|
//        |  1.0|Wed Nov 09 14:12:...|   AnvaMiba|I'm looking for d...|[i'm, looking, fo...|Map(used -> 1.0, ...|          1.0|Map(gans -> 1.727...|     Map(demsar -> 2.3...|
//        |  1.0|Wed Nov 09 14:12:...|   AnvaMiba|I'm looking for d...|[i'm, looking, fo...|Map(used -> 1.0, ...|          2.0|Map(serious -> 1....|     Map(demsar -> 2.3...|
//        |  1.0|Wed Nov 09 14:12:...|   AnvaMiba|I'm looking for d...|[i'm, looking, fo...|Map(used -> 1.0, ...|          3.0|Map(gans -> 1.294...|     Map(demsar -> 2.3...|
//        |  1.0|Sat Nov 12 14:29:...|BafflesSean| Skip navigation ...|[, skip, navigati...|Map(rate -> 1.0, ...|          0.0|Map(serious -> 3....|     Map(demsar -> 2.3...|
//        |  1.0|Sat Nov 12 14:29:...|BafflesSean| Skip navigation ...|[, skip, navigati...|Map(rate -> 1.0, ...|          0.0|Map(serious -> 3....|     Map(demsar -> 2.3...|
//        |  1.0|Sat Nov 12 14:29:...|BafflesSean| Skip navigation ...|[, skip, navigati...|Map(rate -> 1.0, ...|          1.0|Map(gans -> 1.727...|     Map(demsar -> 2.3...|
//        |  1.0|Sat Nov 12 14:29:...|BafflesSean| Skip navigation ...|[, skip, navigati...|Map(rate -> 1.0, ...|          1.0|Map(gans -> 1.727...|     Map(demsar -> 2.3...|
//        |  1.0|Sat Nov 12 14:29:...|BafflesSean| Skip navigation ...|[, skip, navigati...|Map(rate -> 1.0, ...|          2.0|Map(serious -> 1....|     Map(demsar -> 2.3...|
//        |  1.0|Sat Nov 12 14:29:...|BafflesSean| Skip navigation ...|[, skip, navigati...|Map(rate -> 1.0, ...|          2.0|Map(serious -> 1....|     Map(demsar -> 2.3...|
//        |  1.0|Sat Nov 12 14:29:...|BafflesSean| Skip navigation ...|[, skip, navigati...|Map(rate -> 1.0, ...|          3.0|Map(gans -> 1.294...|     Map(demsar -> 2.3...|
//        |  1.0|Sat Nov 12 14:29:...|BafflesSean| Skip navigation ...|[, skip, navigati...|Map(rate -> 1.0, ...|          3.0|Map(gans -> 1.294...|     Map(demsar -> 2.3...|
//        |  3.0|Sun Nov 20 13:50:...| Buck-Nasty|Neural networks a...|[neural, networks...|Map(rate -> 1.0, ...|          0.0|Map(serious -> 3....|     Map(demsar -> 2.3...|
//        |  3.0|Sun Nov 20 13:50:...| Buck-Nasty|Neural networks a...|[neural, networks...|Map(rate -> 1.0, ...|          1.0|Map(gans -> 1.727...|     Map(demsar -> 2.3...|
//        |  3.0|Sun Nov 20 13:50:...| Buck-Nasty|Neural networks a...|[neural, networks...|Map(rate -> 1.0, ...|          2.0|Map(serious -> 1....|     Map(demsar -> 2.3...|
//        |  3.0|Sun Nov 20 13:50:...| Buck-Nasty|Neural networks a...|[neural, networks...|Map(rate -> 1.0, ...|          3.0|Map(gans -> 1.294...|     Map(demsar -> 2.3...|
//        +-----+--------------------+-----------+--------------------+--------------------+--------------------+-------------+--------------------+-------------------------+
//        only showing top 20 rows
//         */
//
//        // UDF to calculate score according to
//        UDF5 calculateScores = new UDF5<Double, scala.collection.immutable.HashMap<String, Double>, scala.collection.immutable.HashMap<String, Double>, scala.collection.immutable.HashMap<String, Double>, BigDecimal, Double> () {
//            public Double call (Double indexed_flair, scala.collection.immutable.HashMap<String, Double> wordFreq, scala.collection.immutable.HashMap<String, Double> statistical_lm, scala.collection.immutable.HashMap<String, Double> background_lm, BigDecimal lambda) {
//                assert lambda.doubleValue() >= 0.0 && lambda.doubleValue() <= 1.0;
//                final Map<String, Double> wordMap = asJavaMap(wordFreq);
//                Double docSize = wordMap.values().stream().mapToDouble(Double::doubleValue).sum();
//                Double alpha = lambda.doubleValue();
//                final Map<String, Double> javaStatisticalLM = asJavaMap(statistical_lm);
//                final Map<String, Double> javaBackgroundModel = asJavaMap(background_lm);
//
//                final Double sum = wordMap.entrySet().parallelStream().map(entry -> {
//                    String curWord = entry.getKey();
//                    Double prior = priors.getOrDefault(indexed_flair, 0.0);
//                    Double cwd = entry.getValue();
//                    Double pwd = javaStatisticalLM.getOrDefault(curWord, 0d);
//                    Double pwc = javaBackgroundModel.getOrDefault(curWord, 0d);
//                    if (pwc != 0) {
//                        Double score = prior * (((1.0d - alpha) * pwd) + (alpha * pwc)) / (alpha * pwc);
////                        System.out.println("score: " + score);
//                        return score;
//                    } else return 0.0d;
//                }).reduce((a, b) -> {
//                    return a + b;
//                }).orElse(0.0);
//                return sum;
//            }
//        };
//        spark.sqlContext().udf().register("calculateScores", calculateScores, DataTypes.DoubleType);
//
//        crossJoined.registerTempTable("crossJoined");
//        final Dataset<Row> withScores = spark.sql(
//                "SELECT " +
//                        "label, " +
//                        "created, " +
//                        "author, " +
//                        "text, " +
//                        "words, " +
//                        "words_freq, " +
//                        "indexed_flair, " +
//                        "statistical_lm, " +
//                        "background_statistical_lm," +
//                        "calculateScores(indexed_flair, words_freq, statistical_lm, background_statistical_lm, 0.5) AS score " +
//                        "FROM crossJoined"
//        );
//        spark.sqlContext().dropTempTable("crossJoined");
//        withScores.printSchema();
//        withScores.orderBy("created", "author", "indexed_flair").show();
//
//        /*
//        root
//         |-- label: double (nullable = true)
//         |-- created: string (nullable = true)
//         |-- author: string (nullable = true)
//         |-- text: string (nullable = true)
//         |-- words: array (nullable = true)
//         |    |-- element: string (containsNull = true)
//         |-- words_freq: map (nullable = true)
//         |    |-- key: string
//         |    |-- value: double (valueContainsNull = true)
//         |-- indexed_flair: double (nullable = true)
//         |-- statistical_lm: map (nullable = true)
//         |    |-- key: string
//         |    |-- value: double (valueContainsNull = true)
//         |-- background_statistical_lm: map (nullable = true)
//         |    |-- key: string
//         |    |-- value: double (valueContainsNull = true)
//         |-- score: double (nullable = true)
//
//        +-----+--------------------+--------------+--------------------+--------------------+--------------------+-------------+--------------------+-------------------------+-------------------+
//        |label|             created|        author|                text|               words|          words_freq|indexed_flair|      statistical_lm|background_statistical_lm|              score|
//        +-----+--------------------+--------------+--------------------+--------------------+--------------------+-------------+--------------------+-------------------------+-------------------+
//        |  0.0|Fri Nov 04 07:40:...|  hammertime89| Cornell Universi...|[, cornell, unive...|Map(al., -> 1.0, ...|          0.0|Map(serious -> 3....|     Map(demsar -> 2.3...| -74.00611925566686|
//        |  0.0|Fri Nov 04 07:40:...|  hammertime89| Cornell Universi...|[, cornell, unive...|Map(al., -> 1.0, ...|          1.0|Map(gans -> 1.727...|     Map(demsar -> 2.3...| -77.88602935799184|
//        |  0.0|Fri Nov 04 07:40:...|  hammertime89| Cornell Universi...|[, cornell, unive...|Map(al., -> 1.0, ...|          2.0|Map(serious -> 1....|     Map(demsar -> 2.3...| -145.4048742365447|
//        |  0.0|Fri Nov 04 07:40:...|  hammertime89| Cornell Universi...|[, cornell, unive...|Map(al., -> 1.0, ...|          3.0|Map(gans -> 1.294...|     Map(demsar -> 2.3...| -348.1995245729181|
//        |  0.0|Fri Nov 04 12:06:...|   schorschico|What do you get i...|[what, do, you, g...|Map(rate -> 2.0, ...|          0.0|Map(serious -> 3....|     Map(demsar -> 2.3...|-117.22709204481293|
//        |  0.0|Fri Nov 04 12:06:...|   schorschico|What do you get i...|[what, do, you, g...|Map(rate -> 2.0, ...|          1.0|Map(gans -> 1.727...|     Map(demsar -> 2.3...| -139.8844304414671|
//        |  0.0|Fri Nov 04 12:06:...|   schorschico|What do you get i...|[what, do, you, g...|Map(rate -> 2.0, ...|          2.0|Map(serious -> 1....|     Map(demsar -> 2.3...|-247.31739492737466|
//        |  0.0|Fri Nov 04 12:06:...|   schorschico|What do you get i...|[what, do, you, g...|Map(rate -> 2.0, ...|          3.0|Map(gans -> 1.294...|     Map(demsar -> 2.3...|  -584.819339355648|
//        |  3.0|Fri Nov 04 12:49:...|julian88888888| 3/4 Free Article...|[, 3/4, free, art...|Map(rate -> 1.0, ...|          0.0|Map(serious -> 3....|     Map(demsar -> 2.3...|-233.90795591895835|
//        |  3.0|Fri Nov 04 12:49:...|julian88888888| 3/4 Free Article...|[, 3/4, free, art...|Map(rate -> 1.0, ...|          1.0|Map(gans -> 1.727...|     Map(demsar -> 2.3...|-288.58826560055417|
//        |  3.0|Fri Nov 04 12:49:...|julian88888888| 3/4 Free Article...|[, 3/4, free, art...|Map(rate -> 1.0, ...|          2.0|Map(serious -> 1....|     Map(demsar -> 2.3...|-479.88171792381706|
//        |  3.0|Fri Nov 04 12:49:...|julian88888888| 3/4 Free Article...|[, 3/4, free, art...|Map(rate -> 1.0, ...|          3.0|Map(gans -> 1.294...|     Map(demsar -> 2.3...|-1135.8229165911016|
//        |  3.0|Fri Nov 04 14:48:...|       afeder_| Home Research Pu...|[, home, research...|Map(rate -> 2.0, ...|          0.0|Map(serious -> 3....|     Map(demsar -> 2.3...|-166.37717351197173|
//        |  3.0|Fri Nov 04 14:48:...|       afeder_| Home Research Pu...|[, home, research...|Map(rate -> 2.0, ...|          1.0|Map(gans -> 1.727...|     Map(demsar -> 2.3...|-206.57234923280404|
//        |  3.0|Fri Nov 04 14:48:...|       afeder_| Home Research Pu...|[, home, research...|Map(rate -> 2.0, ...|          2.0|Map(serious -> 1....|     Map(demsar -> 2.3...|-346.82997126025316|
//        |  3.0|Fri Nov 04 14:48:...|       afeder_| Home Research Pu...|[, home, research...|Map(rate -> 2.0, ...|          3.0|Map(gans -> 1.294...|     Map(demsar -> 2.3...| -827.4326518246219|
//        |  3.0|Fri Nov 04 17:45:...|    spoodmon97|#Stylit allows yo...|[#stylit, allows,...|Map(used -> 1.0, ...|          0.0|Map(serious -> 3....|     Map(demsar -> 2.3...| -26.00316244438291|
//        |  3.0|Fri Nov 04 17:45:...|    spoodmon97|#Stylit allows yo...|[#stylit, allows,...|Map(used -> 1.0, ...|          1.0|Map(gans -> 1.727...|     Map(demsar -> 2.3...|-25.254194793498375|
//        |  3.0|Fri Nov 04 17:45:...|    spoodmon97|#Stylit allows yo...|[#stylit, allows,...|Map(used -> 1.0, ...|          2.0|Map(serious -> 1....|     Map(demsar -> 2.3...| -45.82573829616295|
//        |  3.0|Fri Nov 04 17:45:...|    spoodmon97|#Stylit allows yo...|[#stylit, allows,...|Map(used -> 1.0, ...|          3.0|Map(gans -> 1.294...|     Map(demsar -> 2.3...|-120.35231161876314|
//        +-----+--------------------+--------------+--------------------+--------------------+--------------------+-------------+--------------------+-------------------------+-------------------+
//        only showing top 20 rows
//         */
//
//        final Dataset<Row> toPredict = withScores.select("label", "created", "author", "text", "indexed_flair", "score");
//        toPredict.printSchema();
//        toPredict.orderBy("created", "author", "text", "indexed_flair").show();
//
//        /*
//        root
//         |-- label: double (nullable = true)
//         |-- created: string (nullable = true)
//         |-- author: string (nullable = true)
//         |-- text: string (nullable = true)
//         |-- indexed_flair: double (nullable = true)
//         |-- score: double (nullable = true)
//
//        +-----+--------------------+--------------+--------------------+-------------+-------------------+
//        |label|             created|        author|                text|indexed_flair|              score|
//        +-----+--------------------+--------------+--------------------+-------------+-------------------+
//        |  0.0|Fri Nov 04 07:40:...|  hammertime89| Cornell Universi...|          0.0| -74.00611925566686|
//        |  0.0|Fri Nov 04 07:40:...|  hammertime89| Cornell Universi...|          1.0| -77.88602935799184|
//        |  0.0|Fri Nov 04 07:40:...|  hammertime89| Cornell Universi...|          2.0| -145.4048742365447|
//        |  0.0|Fri Nov 04 07:40:...|  hammertime89| Cornell Universi...|          3.0| -348.1995245729181|
//        |  0.0|Fri Nov 04 12:06:...|   schorschico|What do you get i...|          0.0|-117.22709204481293|
//        |  0.0|Fri Nov 04 12:06:...|   schorschico|What do you get i...|          1.0| -139.8844304414671|
//        |  0.0|Fri Nov 04 12:06:...|   schorschico|What do you get i...|          2.0|-247.31739492737466|
//        |  0.0|Fri Nov 04 12:06:...|   schorschico|What do you get i...|          3.0|  -584.819339355648|
//        |  3.0|Fri Nov 04 12:49:...|julian88888888| 3/4 Free Article...|          0.0|-233.90795591895835|
//        |  3.0|Fri Nov 04 12:49:...|julian88888888| 3/4 Free Article...|          1.0|-288.58826560055417|
//        |  3.0|Fri Nov 04 12:49:...|julian88888888| 3/4 Free Article...|          2.0|-479.88171792381706|
//        |  3.0|Fri Nov 04 12:49:...|julian88888888| 3/4 Free Article...|          3.0|-1135.8229165911016|
//        |  3.0|Fri Nov 04 14:48:...|       afeder_| Home Research Pu...|          0.0|-166.37717351197173|
//        |  3.0|Fri Nov 04 14:48:...|       afeder_| Home Research Pu...|          1.0|-206.57234923280404|
//        |  3.0|Fri Nov 04 14:48:...|       afeder_| Home Research Pu...|          2.0|-346.82997126025316|
//        |  3.0|Fri Nov 04 14:48:...|       afeder_| Home Research Pu...|          3.0| -827.4326518246219|
//        |  3.0|Fri Nov 04 17:45:...|    spoodmon97|#Stylit allows yo...|          0.0| -26.00316244438291|
//        |  3.0|Fri Nov 04 17:45:...|    spoodmon97|#Stylit allows yo...|          1.0|-25.254194793498375|
//        |  3.0|Fri Nov 04 17:45:...|    spoodmon97|#Stylit allows yo...|          2.0| -45.82573829616295|
//        |  3.0|Fri Nov 04 17:45:...|    spoodmon97|#Stylit allows yo...|          3.0|-120.35231161876314|
//        +-----+--------------------+--------------+--------------------+-------------+-------------------+
//        only showing top 20 rows
//         */
//
//        // get max score
//        final Dataset<Row> withMaxScore = toPredict.withColumn("max_score", functions.max("score").over(Window.partitionBy("created", "author")));
//        withMaxScore.printSchema();
//        withMaxScore.show();
//
//        /*
//        root
//         |-- label: double (nullable = true)
//         |-- created: string (nullable = true)
//         |-- author: string (nullable = true)
//         |-- text: string (nullable = true)
//         |-- indexed_flair: double (nullable = true)
//         |-- score: double (nullable = true)
//         |-- max_score: double (nullable = true)
//
//        +-----+--------------------+-------------------+--------------------+-------------+-------------------+------------------+
//        |label|             created|             author|                text|indexed_flair|              score|         max_score|
//        +-----+--------------------+-------------------+--------------------+-------------+-------------------+------------------+
//        |  1.0|Mon Nov 14 10:15:...|         Mandrathax|This is a place t...|          0.0| -47.54689376280095|-47.18677502636149|
//        |  1.0|Mon Nov 14 10:15:...|         Mandrathax|This is a place t...|          1.0| -47.18677502636149|-47.18677502636149|
//        |  1.0|Mon Nov 14 10:15:...|         Mandrathax|This is a place t...|          3.0|-221.33203214563878|-47.18677502636149|
//        |  1.0|Mon Nov 14 10:15:...|         Mandrathax|This is a place t...|          2.0| -89.53472385190119|-47.18677502636149|
//        |  0.0|Tue Oct 11 17:22:...|     rmltestaccount| LI YAO et al.: O...|          0.0| -647.1849626287669|-647.1849626287669|
//        |  0.0|Tue Oct 11 17:22:...|     rmltestaccount| LI YAO et al.: O...|          1.0| -802.1784950518547|-647.1849626287669|
//        |  0.0|Tue Oct 11 17:22:...|     rmltestaccount| LI YAO et al.: O...|          3.0| -3085.929471435863|-647.1849626287669|
//        |  0.0|Tue Oct 11 17:22:...|     rmltestaccount| LI YAO et al.: O...|          2.0|-1313.2488814661324|-647.1849626287669|
//        |  1.0|Mon Nov 28 00:08:...|darkconfidantislife|Hey there guys,
//
//        ...|          0.0| -34.76261479297029|-32.70491276362627|
//        |  1.0|Mon Nov 28 00:08:...|darkconfidantislife|Hey there guys,
//
//        ...|          1.0| -32.70491276362627|-32.70491276362627|
//        |  1.0|Mon Nov 28 00:08:...|darkconfidantislife|Hey there guys,
//
//        ...|          3.0|-155.85139039768495|-32.70491276362627|
//        |  1.0|Mon Nov 28 00:08:...|darkconfidantislife|Hey there guys,
//
//        ...|          2.0|-60.185771458269286|-32.70491276362627|
//        |  2.0|Mon Nov 28 10:21:...|             dtraxl|DeepGraph is a sc...|          0.0| -74.34824862903571|-74.34824862903571|
//        |  2.0|Mon Nov 28 10:21:...|             dtraxl|DeepGraph is a sc...|          1.0| -78.35292192330006|-74.34824862903571|
//        |  2.0|Mon Nov 28 10:21:...|             dtraxl|DeepGraph is a sc...|          3.0| -350.2130078819673|-74.34824862903571|
//        |  2.0|Mon Nov 28 10:21:...|             dtraxl|DeepGraph is a sc...|          2.0|-146.40758298846634|-74.34824862903571|
//        |  0.0|Mon Nov 28 17:45:...|          omoindrot| Sebastian Ruder ...|          0.0| -728.5958904794616| -534.707779547275|
//        |  0.0|Mon Nov 28 17:45:...|          omoindrot|In past blog post...|          0.0|  -534.707779547275| -534.707779547275|
//        |  0.0|Mon Nov 28 17:45:...|          omoindrot| Sebastian Ruder ...|          1.0| -887.7491507269766| -534.707779547275|
//        |  0.0|Mon Nov 28 17:45:...|          omoindrot|In past blog post...|          1.0| -665.6320664590793| -534.707779547275|
//        +-----+--------------------+-------------------+--------------------+-------------+-------------------+------------------+
//        only showing top 20 rows
//         */
//
//        // get prediction associated with max score
//        final Dataset<Row> withPrediction = withMaxScore.where(withMaxScore.col("max_score").equalTo(withMaxScore.col("score"))).withColumnRenamed("indexed_flair", "prediction");
//        withPrediction.printSchema();
//        withPrediction.show();
//
//        /*
//        root
//         |-- label: double (nullable = true)
//         |-- created: string (nullable = true)
//         |-- author: string (nullable = true)
//         |-- text: string (nullable = true)
//         |-- prediction: double (nullable = true)
//         |-- score: double (nullable = true)
//         |-- max_score: double (nullable = true)
//
//        +-----+--------------------+-------------------+--------------------+----------+-------------------+-------------------+
//        |label|             created|             author|                text|prediction|              score|          max_score|
//        +-----+--------------------+-------------------+--------------------+----------+-------------------+-------------------+
//        |  1.0|Mon Nov 14 10:15:...|         Mandrathax|This is a place t...|       1.0| -47.18677502636149| -47.18677502636149|
//        |  0.0|Tue Oct 11 17:22:...|     rmltestaccount| LI YAO et al.: O...|       0.0| -647.1849626287669| -647.1849626287669|
//        |  1.0|Mon Nov 28 00:08:...|darkconfidantislife|Hey there guys,
//
//        ...|       1.0| -32.70491276362627| -32.70491276362627|
//        |  2.0|Mon Nov 28 10:21:...|             dtraxl|DeepGraph is a sc...|       0.0| -74.34824862903571| -74.34824862903571|
//        |  0.0|Mon Nov 28 17:45:...|          omoindrot|In past blog post...|       0.0|  -534.707779547275|  -534.707779547275|
//        |  0.0|Fri Nov 11 12:03:...|       downtownslim| Under review as ...|       0.0|  -647.184962628767|  -647.184962628767|
//        |  1.0|Sat Nov 26 08:09:...|              cptai|I am reading the ...|       1.0| -20.17689598547549| -20.17689598547549|
//        |  1.0|Thu Nov 17 14:57:...|        bronzestick|In several applic...|       1.0| -42.94189034717664| -42.94189034717664|
//        |  0.0|Tue Oct 18 10:21:...|             tuan3w| Cornell Universi...|       0.0| -79.89321736309498| -79.89321736309498|
//        |  1.0|Sat Nov 05 04:15:...|        wjbianjason|To be specific,wh...|       1.0|-12.573424782221249|-12.573424782221249|
//        |  1.0|Thu Oct 13 16:34:...|           Pieranha|The Densely Conne...|       1.0| -36.21456066940183| -36.21456066940183|
//        |  1.0|Wed Oct 19 20:12:...| frustrated_lunatic|In Liu CiXin’s no...|       1.0| -53.00182763938054| -53.00182763938054|
//        |  2.0|Mon Nov 28 19:35:...|           Weihua99| Skip to content ...|       0.0| -48.11865459561534| -48.11865459561534|
//        |  0.0|Mon Nov 14 13:03:...|          jhartford|For details and a...|       0.0| -8.039219933126056| -8.039219933126056|
//        |  1.0|Tue Nov 08 10:52:...|           huyhcmut|How can I train a...|       1.0|-12.573424782221249|-12.573424782221249|
//        |  2.0|Tue Oct 25 14:57:...|      shagunsodhani| Skip to content ...|       0.0| -85.83494706485156| -85.83494706485156|
//        |  3.0|Wed Nov 16 16:30:...|             clbam8|Here at AYLIEN we...|       0.0|-133.20035411396975|-133.20035411396975|
//        |  1.0|Tue Oct 25 20:49:...|           jayjaymz|Hello there. I'm ...|       1.0| -35.05147929983538| -35.05147929983538|
//        |  1.0|Tue Nov 29 07:05:...|             Kiuhnm|I'm reading Mansi...|       1.0|-28.675704273027858|-28.675704273027858|
//        |  2.0|Tue Nov 29 23:33:...|        longinglove|Can we segment un...|       0.0| -79.35532502384983| -79.35532502384983|
//        +-----+--------------------+-------------------+--------------------+----------+-------------------+-------------------+
//        only showing top 20 rows
//         */
//
//        // index predictions and labels
//        final Dataset<Row> predictionsAndLabels = withPrediction.select("prediction", "label");
//
//        predictionsAndLabels.printSchema();
//        predictionsAndLabels.show();
//
//        /*
//        root
//         |-- prediction: double (nullable = true)
//         |-- label: double (nullable = true)
//
//        +----------+-----+
//        |prediction|label|
//        +----------+-----+
//        |       1.0|  1.0|
//        |       0.0|  0.0|
//        |       1.0|  1.0|
//        |       0.0|  2.0|
//        |       0.0|  0.0|
//        |       0.0|  0.0|
//        |       1.0|  1.0|
//        |       1.0|  1.0|
//        |       0.0|  0.0|
//        |       1.0|  1.0|
//        |       1.0|  1.0|
//        |       1.0|  1.0|
//        |       0.0|  2.0|
//        |       0.0|  0.0|
//        |       1.0|  1.0|
//        |       0.0|  2.0|
//        |       0.0|  3.0|
//        |       1.0|  1.0|
//        |       1.0|  1.0|
//        |       0.0|  2.0|
//        +----------+-----+
//        only showing top 20 rows
//         */
//
//        JavaRDD<Row> predictionAndLabelRowRDD = predictionsAndLabels.select("prediction", "label").toJavaRDD();
//
//        final JavaRDD<Tuple2<Object, Object>> predictionAndLabelRDD = predictionAndLabelRowRDD.map(new Function<Row, Tuple2<Object, Object>>() {
//            public Tuple2<Object, Object> call(Row row) {
//                Double prediction = row.getDouble(0);
//                Double label = row.getDouble(1);
//                return new Tuple2<Object, Object>(prediction, label);
//            }
//        });
//
//        // https://spark.apache.org/docs/latest/mllib-evaluation-metrics.html
//        // Get evaluation metrics
//        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabelRDD.rdd());
//
//        // Confusion matrix
//        Matrix confusion = metrics.confusionMatrix();
//        System.out.println("Confusion matrix: \n" + confusion);
//
//        /*
//        Confusion matrix:
//        138.0  22.0   0.0  0.0
//        33.0   120.0  0.0  0.0
//        64.0   47.0   0.0  0.0
//        27.0   10.0   0.0  0.0
//         */
//
//        // Overall statistics
//        System.out.println("Accuracy = " + metrics.accuracy());
//
//        /*
//        Accuracy = 0.559652928416486
//        */
//
//
//        // Stats by labels
//        for (int i = 0; i < metrics.labels().length; i++) {
//            System.out.format("Class %f precision = %f\n", metrics.labels()[i],metrics.precision(
//                    metrics.labels()[i]));
//            System.out.format("Class %f recall = %f\n", metrics.labels()[i], metrics.recall(
//                    metrics.labels()[i]));
//            System.out.format("Class %f F1 score = %f\n", metrics.labels()[i], metrics.fMeasure(
//                    metrics.labels()[i]));
//        }
//
//        /*
//        Class 0.000000 precision = 0.526718
//        Class 0.000000 recall = 0.862500
//        Class 0.000000 F1 score = 0.654028
//        Class 1.000000 precision = 0.603015
//        Class 1.000000 recall = 0.784314
//        Class 1.000000 F1 score = 0.681818
//        Class 2.000000 precision = 0.000000
//        Class 2.000000 recall = 0.000000
//        Class 2.000000 F1 score = 0.000000
//        Class 3.000000 precision = 0.000000
//        Class 3.000000 recall = 0.000000
//        Class 3.000000 F1 score = 0.000000
//         */
//
//        //Weighted stats
//        System.out.format("Weighted precision = %f\n", metrics.weightedPrecision());
//        System.out.format("Weighted recall = %f\n", metrics.weightedRecall());
//        System.out.format("Weighted F1 score = %f\n", metrics.weightedFMeasure());
//        System.out.format("Weighted false positive rate = %f\n", metrics.weightedFalsePositiveRate());
//
//        /*
//        Weighted precision = 0.382942
//        Weighted recall = 0.559653
//        Weighted F1 score = 0.453281
//        Weighted false positive rate = 0.228107
//         */
//
//
//        System.out.println("priors");
//        priors.entrySet().forEach(entry -> {
//            System.out.println(entry);
//        });
//
//        /*
//        priors
//        1.0=0.32762312633832974
//        2.0=0.24197002141327623
//        0.0=0.3468950749464668
//        3.0=0.0835117773019272
//         */
//
//        System.out.println("indexed_flair labels...");
//        for(String label : flairStringIndexerModel.labels()) {
//            System.out.println(label);
//        }
//
//        /*
//        indexed_flair labels...
//        three
//        one
//        four
//        two
//         */
//
//        // 1.0 translates to three
//        // 2.0 translates to one
//        // 0.0 translates to four
//        // 3.0 translates to two
//    }
//
//}
