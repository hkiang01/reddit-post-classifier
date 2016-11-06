package com.cs410dso.postclassifier.ingestion;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimaps;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import net.dean.jraw.http.HttpRequest;
import net.dean.jraw.http.RequestBody;
import net.dean.jraw.models.Flair;
import net.dean.jraw.models.Submission;

import org.apache.commons.collections.HashBag;
import org.apache.commons.io.IOUtils;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import jersey.repackaged.com.google.common.collect.HashMultiset;
import scala.collection.AbstractSeq;

/**
 * Facilitates the ingestion of {@link net.dean.jraw.models.Submission} from a {@link Collection} of subreddits using a set of custom filtering rules.
 */
public class FilteredSubredditIngestion extends SubredditIngestion {

    private static final String juicerPrependURL = "https://juicer.herokuapp.com/api/article?url=";

    /**
     * Instantiates a new FilteredSubredditIngestion
     */
    public FilteredSubredditIngestion() {
        super();
    }

    /**
     * Instantiates a new FilteredSubredditIngestion
     * @param subreddits A {@link Collection} of subreddits to ingest
     * @param limit The number of {@link net.dean.jraw.models.Submission}s to ingest and filter through
     */
    public FilteredSubredditIngestion(Collection<String> subreddits, int limit) {
        super(subreddits, limit);
    }

    /**
     * Maps {@link Submission}s as to whether or not they are stickied
     * @return a {@link ImmutableMultimap} whose true and false members are and are not stickied, respectively.
     * @see <a href="https://github.com/google/guava/wiki/CollectionUtilitiesExplained#multimaps">https://github.com/google/guava/wiki/CollectionUtilitiesExplained#multimaps</a>
     */
    public ImmutableListMultimap<Boolean, Submission> getSubmissionsByStickied() {
        ImmutableCollection<Submission> unfiltered =  this.getSubmissions();
        Function<Submission, Boolean> isStickiedFunction = new Function<Submission, Boolean>() {
            public Boolean apply(Submission submission) {
                return submission.isStickied();
            }
        };
        return Multimaps.index(unfiltered, isStickiedFunction);
    }

    /**
     * Maps {@link Submission}s by their domain
     * @return a {@link ImmutableMultimap} whose members are indexed by their domain
     * @see <a href="https://github.com/google/guava/wiki/CollectionUtilitiesExplained#multimaps">https://github.com/google/guava/wiki/CollectionUtilitiesExplained#multimaps</a>
     */
    public ImmutableListMultimap<String, Submission> getSubmissionsByDomain() {
        ImmutableCollection<Submission> unfiltered =  this.getSubmissions();
        Function<Submission, String> domainFunction = new Function<Submission, String>() {
            public String apply(Submission submission) {
                return submission.getDomain();
            }
        };
        return Multimaps.index(unfiltered, domainFunction);
    }

    /**
     * Maps {@link Submission}s as to whether or not they are from their respective self.subreddit domain
     * @return a {@link ImmutableListMultimap} whose members are indexed by whether or not they are from their respective self.subreddit domain
     */
    public ImmutableListMultimap<Boolean, Submission> getSubmissionsBySelf() {
        // Java stream tutorial examples: http://winterbe.com/posts/2014/07/31/java8-stream-tutorial-examples/
        ImmutableCollection<Submission> submissions = this.getSubmissions();
        Collection<String> selfDomains = this.getSubredditSelfDomains();
        Collection<String> lowercaseSelfDomains = selfDomains.stream()
                .map(String::toLowerCase)
                .collect(Collectors.toCollection(TreeSet::new));
        Function<Submission, Boolean> selfFunction = new Function<Submission, Boolean>() {
            public Boolean apply(Submission submission) {
                String domain = submission.getDomain().toLowerCase();
                return lowercaseSelfDomains.contains(domain);
            }
        };
        return Multimaps.index(submissions, selfFunction);
    }

    /**
     * Get text and flair for every submission
     * @return a {@link Collection} containing {@link java.util.AbstractMap.SimpleEntry} of {@link Submission}s and {@link TextFlair}s
     */
    public Collection<AbstractMap.SimpleEntry<Submission, TextFlair>> getSubmissionsTextFlair() {
        ImmutableListMultimap<Boolean, Submission> submissions = getSubmissionsBySelf();
        return submissions.entries().parallelStream().map(e -> { // parallel streams
            Submission curSubmission = e.getValue();
            if (e.getKey()) { // if the entry's domain is from self.subreddit
                TextFlair submissionTextFlair = new TextFlair(curSubmission.getSelftext(), curSubmission.getSubmissionFlair());
                return new AbstractMap.SimpleEntry<Submission, TextFlair>(curSubmission, submissionTextFlair);
            } else {
                String url = curSubmission.getUrl();
                String text = getSubmissionArticleTextViajuicer(url);
                TextFlair submissionTextFlair = new TextFlair(text, curSubmission.getSubmissionFlair());
                return new AbstractMap.SimpleEntry<Submission, TextFlair>(curSubmission, submissionTextFlair);
            }
        }).collect(Collectors.toCollection(HashSet::new)); // hashset for constant time
    }

    /**
     * Uses juicer to grab the article body of an article
     * @param urlString the URL to plug into juicer API
     * @return the body result of the juicer API call
     * @see <a href="https://juicer.herokuapp.com/">juicer</a>
     * WARNING: Does not work for all sites and document types, e.g., PDFs
     */
    public String getSubmissionArticleTextViajuicer(String urlString) {
        try{
            String result =  IOUtils.toString(new URL(juicerPrependURL + urlString).openStream());
            JsonParser jsonParser = new JsonParser();
            JsonElement element = jsonParser.parse(result);
            JsonObject jsonObject = element.getAsJsonObject();
            return jsonObject.get("article").getAsJsonObject().get("body").getAsString(); // root -> article -> body
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }

    }

    /**
     * Used to store text and flair of a {@link Submission}
     */
    public class TextFlair {

        /**
         * The text
         */
        private String text;

        /**
         * The {@link Flair}
         */
        private Flair flair;

        /**
         * Parameterized constructor
         * @param text A {@link String} of text
         * @param flair A {@link Flair}
         */
        TextFlair(String text, Flair flair) {
            this.text = text;
            this.flair = flair;
        }

        /**
         * A {@link String} representation of a {@link TextFlair}
         * @return The {@link String} representation
         */
        public String toString() {
            return "flair: " + flair.getCssClass() + "\t" + flair.getText() + "\ntext: " + text;
        }

    }

}
