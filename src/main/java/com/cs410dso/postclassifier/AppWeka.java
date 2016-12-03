package com.cs410dso.postclassifier;

import com.cs410dso.postclassifier.ingestion.FilteredSubredditIngestion;
import java.util.*;

    /**
     * AppWeka main for weka
     */
public class AppWeka {

    public static void main(String[] args) {

        // scrape and in¡gest
        ArrayList<String> listOfSubreddits = new ArrayList();
        listOfSubreddits.add("machinelearning");
        FilteredSubredditIngestion filteredSubredditIngestion = new FilteredSubredditIngestion(listOfSubreddits, 1000);
        filteredSubredditIngestion.saveSubmissionsAsTxtUnderClassDirectories(); // save in

    }

}
