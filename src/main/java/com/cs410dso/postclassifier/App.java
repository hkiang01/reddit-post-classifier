package com.cs410dso.postclassifier;

import com.cs410dso.postclassifier.ingestion.SubredditIngestion;
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
        SubredditIngestion ingestion = new SubredditIngestion(listOfSubreddits, 5);
        Collection<Submission> submissions = ingestion.getSubmissions();
        submissions.forEach(l -> System.out.println(l.toString()));
    }
}
