package com.cs410dso.postclassifier;

import com.cs410dso.postclassifier.ingestion.SubredditIngestion;
import net.dean.jraw.models.LoggedInAccount;

/**
 * App main
 */
public class App {
    public static void main(String[] args) {

        SubredditIngestion ingestion = new SubredditIngestion("machinelearning");
        LoggedInAccount account = ingestion.getRedditClient().me();
        int numKarma = account.getCommentKarma();
        String username = ingestion.getUsername();
        System.out.println(username + " has " + numKarma + " comment karma!");
    }
}
