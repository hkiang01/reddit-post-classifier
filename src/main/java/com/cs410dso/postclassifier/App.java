package com.cs410dso.postclassifier;

import com.cs410dso.postclassifier.ingestion.SubredditIngestion;
import net.dean.jraw.models.Listing;
import net.dean.jraw.models.Submission;
import net.dean.jraw.paginators.SubredditPaginator;

import java.util.*;

/**
 * App main
 */
public class App {
    public static void main(String[] args) {
        List<String> listOfSubreddits = new ArrayList<>();
        listOfSubreddits.add("machinelearning");
        listOfSubreddits.add("UIUC");
        SubredditIngestion ingestion = new SubredditIngestion(listOfSubreddits);
        System.out.println("subreddit: " + ingestion.getSubredditPaginator().getSubreddit());

        ingestion.addSubreddit("pics");
        System.out.println("subreddit: " + ingestion.getSubredditPaginator().getSubreddit());

        Listing<Submission> listing = ingestion.getSubredditPaginator().next();
        listing.forEach( l -> System.out.println(l.toString()));
    }
}
