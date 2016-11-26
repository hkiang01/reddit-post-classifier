package com.cs410dso.postclassifier;

import com.cs410dso.postclassifier.ingestion.FilteredSubredditIngestion;
import java.util.ArrayList;
import java.util.List;
/**
 * App main
 */
public class App {
    public static void main(String[] args) {
        List<String> listOfSubreddits = new ArrayList<>();
        listOfSubreddits.add("machinelearning");
        FilteredSubredditIngestion ingestion = new FilteredSubredditIngestion(listOfSubreddits, 50);
        ingestion.saveSubmissionAndMetadataAboveThresholdAsJson();


//        SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        int NUM_SAMPLES = 100000000;
//        List<Integer> l = new ArrayList<Integer>(NUM_SAMPLES);
//        for (int i = 0; i < NUM_SAMPLES; i++) {
//            l.add(i);
//        }
//
//        long count = sc.parallelize(l).filter(new Function<Integer, Boolean>() {
//            public Boolean call(Integer i) {
//                double x = Math.random();
//                double y = Math.random();
//                return x*x + y*y < 1;
//            }
//        }).count();
//        System.out.println("Pi is roughly " + 4.0 * count / NUM_SAMPLES);

    }
}
