package com.cs410dso.postclassifier.ingestion;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimaps;
import net.dean.jraw.models.Submission;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import scala.collection.AbstractSeq;

/**
 * Facilitates the ingestion of {@link net.dean.jraw.models.Submission} from a {@link Collection} of subreddits using a set of custom filtering rules.
 */
public class FilteredSubredditIngestion extends SubredditIngestion {

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


}
