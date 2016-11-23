package com.cs410dso.postclassifier;

import com.google.common.collect.ImmutableListMultimap;

import com.cs410dso.postclassifier.ingestion.FilteredSubredditIngestion;

import net.dean.jraw.models.Submission;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * App main
 */
public class App {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        int NUM_SAMPLES = 100000000;
        List<Integer> l = new ArrayList<Integer>(NUM_SAMPLES);
        for (int i = 0; i < NUM_SAMPLES; i++) {
            l.add(i);
        }

        long count = sc.parallelize(l).filter(new Function<Integer, Boolean>() {
            public Boolean call(Integer i) {
                double x = Math.random();
                double y = Math.random();
                return x*x + y*y < 1;
            }
        }).count();
        System.out.println("Pi is roughly " + 4.0 * count / NUM_SAMPLES);

//        List<String> listOfSubreddits = new ArrayList<>();
//        listOfSubreddits.add("machinelearning");
//        FilteredSubredditIngestion ingestion = new FilteredSubredditIngestion(listOfSubreddits, 50);
//        Collection<AbstractMap.SimpleEntry<Submission, FilteredSubredditIngestion.UrlAuthorFlairMethodText>> stf = ingestion.getSubmissionsTextFlair();
//        stf.forEach(e -> System.out.println(e.getValue().toString()));
//        ingestion.getSubmissions().stream().forEach(e -> System.out.println(e.toString()));

    }
}
