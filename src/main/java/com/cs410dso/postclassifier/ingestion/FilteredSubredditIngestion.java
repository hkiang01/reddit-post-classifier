package com.cs410dso.postclassifier.ingestion;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimaps;
import net.dean.jraw.models.Submission;
import java.util.Collection;

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
    public ImmutableListMultimap<String, Submission> getSubmissionByDomain() {
        ImmutableCollection<Submission> unfiltered =  this.getSubmissions();
        Function<Submission, String> domainFunction = new Function<Submission, String>() {
            public String apply(Submission submission) {
                return submission.getDomain();
            }
        };
        return Multimaps.index(unfiltered, domainFunction);
    }


}
