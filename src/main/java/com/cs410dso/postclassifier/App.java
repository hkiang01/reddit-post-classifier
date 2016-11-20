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
        List<String> listOfSubreddits = new ArrayList<>();
        listOfSubreddits.add("machinelearning");
        FilteredSubredditIngestion ingestion = new FilteredSubredditIngestion(listOfSubreddits, 5);
        Collection<AbstractMap.SimpleEntry<Submission, FilteredSubredditIngestion.UrlAuthorFlairMethodText>> stf = ingestion.getSubmissionsTextFlair();
        stf.forEach(e -> System.out.println(e.getValue().toString()));
//        ingestion.getSubmissions().stream().forEach(e -> System.out.println(e.toString()));

    }
}
