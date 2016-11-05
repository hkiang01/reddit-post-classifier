package com.cs410dso.postclassifier;

import com.google.common.collect.ImmutableListMultimap;

import com.cs410dso.postclassifier.ingestion.FilteredSubredditIngestion;

import net.dean.jraw.models.Submission;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * App main
 */
public class App {
    public static void main(String[] args) {
        List<String> listOfSubreddits = new ArrayList<>();
        listOfSubreddits.add("machinelearning");
        FilteredSubredditIngestion ingestion = new FilteredSubredditIngestion(listOfSubreddits, 10);
        ImmutableListMultimap<Boolean, Submission> selfOrNot = ingestion.getSubmissionsBySelf();
        selfOrNot.entries().forEach(s -> System.out.println(s));
    }
}
