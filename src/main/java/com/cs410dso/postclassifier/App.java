package com.cs410dso.postclassifier;

import com.google.common.collect.ImmutableListMultimap;

import com.cs410dso.postclassifier.ingestion.FilteredSubredditIngestion;

import net.dean.jraw.models.Submission;

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.json.simple.JSONObject;

import java.io.BufferedOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ResourceBundle;

import jdk.nashorn.internal.parser.JSONParser;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

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
