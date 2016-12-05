package com.cs410dso.postclassifier.util;

import com.cs410dso.postclassifier.ingestion.FilteredSubredditIngestion;
import net.dean.jraw.models.Submission;

import java.util.AbstractMap;

/**
 * A wrapper class that contains the submission and its associated url, author, flair, method of extraction, and text
 * (used in {@link FilteredSubredditIngestion}
 */
public class SubmissionAndTextWrapper {
    public final AbstractMap.SimpleEntry<Submission, FilteredSubredditIngestion.UrlAuthorFlairMethodText> item;
    public SubmissionAndTextWrapper(AbstractMap.SimpleEntry<Submission, FilteredSubredditIngestion.UrlAuthorFlairMethodText> code) {
        this.item = code;
    }
}
