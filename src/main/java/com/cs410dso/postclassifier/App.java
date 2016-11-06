package com.cs410dso.postclassifier;

import com.google.common.collect.ImmutableListMultimap;

import com.cs410dso.postclassifier.ingestion.FilteredSubredditIngestion;

import net.dean.jraw.models.Submission;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * App main
 */
public class App {
    public static void main(String[] args) {
        System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.Jdk14Logger");
        List<String> listOfSubreddits = new ArrayList<>();
        listOfSubreddits.add("machinelearning");
        FilteredSubredditIngestion ingestion = new FilteredSubredditIngestion(listOfSubreddits, 2);
        Collection<AbstractMap.SimpleEntry<Submission, FilteredSubredditIngestion.TextFlair>> stf = ingestion.getSubmissionsTextFlair();
        ingestion.getSubmissionsTextFlair().stream().forEach(e -> System.out.println(e.getValue().toString()));

    }
}
