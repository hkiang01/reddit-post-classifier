package com.cs410dso.postclassifier;

import com.cs410dso.postclassifier.ingestion.FilteredSubredditIngestion;
import com.cs410dso.postclassifier.model.LocalSubredditFlairModel;

import java.util.*;

    /**
     * AppWeka main for weka
     */
public class AppWeka {

    public static void main(String[] args) {

        // scrape and in¡gest
        ArrayList<String> listOfSubreddits = new ArrayList();
        listOfSubreddits.add("machinelearning");
        LocalSubredditFlairModel localSubredditFlairModel = new LocalSubredditFlairModel();
        localSubredditFlairModel.saveSubmissionsAsTxtUnderClassDirectories(); // save in

    }

}
