package com.cs410dso.postclassifier;

import com.cs410dso.postclassifier.ingestion.SubredditIngestion;
import net.dean.jraw.models.Listing;
import net.dean.jraw.models.LoggedInAccount;
import net.dean.jraw.models.Submission;
import net.dean.jraw.paginators.SubredditPaginator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * App main
 */
public class App {
    public static void main(String[] args) {
        SubredditIngestion ingestion = new SubredditIngestion("machinelearning");
        SubredditPaginator paginator = ingestion.getSubredditPaginator();
        System.out.println("subreddit: " + paginator.getSubreddit());

        Listing<Submission> listing = ingestion.getSubredditPaginator().next();
        listing.forEach( l -> System.out.println(l.toString()));
    }
}
