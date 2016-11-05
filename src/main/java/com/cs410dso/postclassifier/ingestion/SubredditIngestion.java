package com.cs410dso.postclassifier.ingestion;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;
import net.dean.jraw.RedditClient;
import net.dean.jraw.http.UserAgent;
import net.dean.jraw.http.oauth.Credentials;
import net.dean.jraw.http.oauth.OAuthData;
import net.dean.jraw.http.oauth.OAuthException;
import net.dean.jraw.models.Listing;
import net.dean.jraw.models.Submission;
import net.dean.jraw.paginators.SubredditPaginator;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This class provides an easy way to ingest the contents of a given subreddit.
 * This uses the {@link RedditClient} API
 * @see RedditClient
 */
public class SubredditIngestion {

    /**
     * The {@link RedditClient} used to facilitate the ingestion. Only 1 redditClient per user, a singleton.
     */
    private static RedditClient redditClient;

    /**
     * The subreddit to ingest
     */
    private Collection<String> subreddits;

    /**
     * The combined subreddits, e.g., A+B is subreddit A and B
     */
    private String combinedSubreddits;

    /**
     * The number of submissions (posts) to ingest. See {@link SubredditPaginator}
     */
    private int limit = 25;

    /**
     * The {@link SubredditPaginator} used to facilitate the ingestion
     */
    private SubredditPaginator subredditPaginator;

    /**
     * A {@link ImmutableCollection} containing {@link Submission}s from the {@link SubredditPaginator}
     */
    private ImmutableCollection<Submission> submissions;

    /**
     * Instantiates a new SubredditIngestion
     */
    public SubredditIngestion() {
        subredditIngestionConstructorHelper();
    }

    /**
     * Instantiates a new SubredditIngestion
     * @param subreddits A {@link Collection} of subreddits to ingest
     * @param limit The number of {@link net.dean.jraw.models.Submission}s to ingest
     */
    public SubredditIngestion(Collection<String> subreddits, int limit) {
        // generate the subredditPaginator
        this.subreddits = subreddits;
        this.limit = limit;
        subredditIngestionConstructorHelper();
    }

    /**
     * Facilitates the default and parameterized constructors for {@link #SubredditIngestion()}
     */
    private void subredditIngestionConstructorHelper() {
        // Descriptive User-Agent header required by Reddit API (https://github.com/thatJavaNerd/JRAW/wiki/Quickstart)
        UserAgent myUserAgent = createUserAgent();
        redditClient = new RedditClient(myUserAgent);

        // OAuth Credentials (https://thatjavanerd.github.io/JRAW/docs/latest/net/dean/jraw/http/oauth/Credentials.html)
        Credentials credentials = getCredentials();
        try {
            OAuthData authData = redditClient.getOAuthHelper().easyAuth(credentials);
            // notify the RedditClient that we have been authorized
            redditClient.authenticate(authData);
        } catch (OAuthException e) {
            System.out.println("Invalid credentials in resources/credentials.json");
            e.printStackTrace();
        }
        // generate the subredditPaginator
        generateSubredditPaginator(this.limit);

        // ingest submissions
        ingestSubmissions();
    }

    /**
     * Gets the reddicClient
     * @return the {@link RedditClient} from this {{@link #SubredditIngestion()}}
     */
    public RedditClient getRedditClient() {
        return redditClient;
    }

    /**
     * Gets the username
     * @return the username {@link String} from this {{@link #SubredditIngestion()}}
     */
    public String getUsername() {
        return getCredentials().getUsername();
    }

    /**
     * Gets subreddits of {{@link #SubredditIngestion()}}
     * @return The {@link Collection} of {@link String}s of subreddits
     */
    public Collection<String> getSubreddits() {
        return this.subreddits;
    }

    /**
     * Gets the self domains of each subreddit in the {@link SubredditIngestion}
     * @return a {@link Collection} of {@link String}s of self domains per subreddit
     */
    public Collection<String> getSubredditSelfDomains() {
        Collection<String> selfSubreddits = this.getSubreddits().stream()
                //.map(String::toLowerCase)
                .map(e -> "self." + e)
                .collect(Collectors.toCollection(TreeSet::new));
        return selfSubreddits;
    }

    /** Adds to subreddits */

    /**
     * Adds to subreddits of {{@link #SubredditIngestion()}}
     * @param  subreddit A {@link String} representing a subreddit to add
     * @return A {@link SubredditPaginator} with subreddit added
     */
    public SubredditPaginator addSubreddit(String subreddit) {
        this.subreddits.add(subreddit);
        generateSubredditPaginator(this.limit);
        ingestSubmissions();
        return this.subredditPaginator;
    }

    /**
     * Gets the {@link SubredditPaginator} of the {{@link #SubredditIngestion()}}
     * @return The {@link SubredditPaginator}
     */
    public SubredditPaginator getSubredditPaginator() { return this.subredditPaginator; }

    /** Gets the raw submissions */

    /**
     * Gets the raw {@link Submission}s of the {{@link #SubredditIngestion()}}
     * @return A {@link Collection} of {@link Submission}s
     */
    public ImmutableCollection<Submission> getSubmissions() {
        return this.submissions;
    }

    /** Used to fetch the project properties from pom.xml */

    /**
     * Fetches the project properties from pom.xml
     * @return A {@link ProjectProperties} object containing groupId, artifactId, and version as {@link String}s
     */
    private ProjectProperties getProjectProperties() {
        // http://stackoverflow.com/questions/26551439/getting-maven-project-version-and-artifact-id-from-pom-while-running-in-eclipse
        final Properties properties = new Properties();
        try {
            properties.load(this.getClass().getResourceAsStream("/project.properties"));
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Failed to find project.properties, unable to ingest groupId, artifactId, version.");
        }

        return new ProjectProperties(
                properties.getProperty("groupId"),
                properties.getProperty("artifactID"),
                properties.getProperty("version"));
    }

    /**
     * Gets the credentials in order using {@link Credentials}
     * @return The {@link Credentials} associated with the fields present in resources/credentials.json
     */
    private Credentials getCredentials() {
        JSONParser parser = new JSONParser();
        String username = "";
        String password = "";
        String clientId = "";
        String clientSecret = "";

        try {
            Object obj = parser.parse(new InputStreamReader(getClass().getResourceAsStream("/credentials.json")));
            JSONObject jsonObject = (JSONObject) obj;

            username = (String) jsonObject.get("username");
            password = (String) jsonObject.get("password");
            clientId = (String) jsonObject.get("clientId");
            clientSecret = (String) jsonObject.get("clientSecret");

        } catch (IOException e) {
            System.out.println("Please specify credentials in resources/credentials.json\n" +
                    "Specify 'username', 'password', 'clientId', 'clientSecret'\n" +
                    "See: https://github.com/reddit/reddit/wiki/OAuth2\n" +
                    "See: https://github.com/thatJavaNerd/JRAW/wiki/OAuth2");
            e.printStackTrace();
        } catch (ParseException e) {
            System.out.println("Unable to read json file in resources/credentials.json");
            e.printStackTrace();
        }

        return Credentials.script(username, password, clientId, clientSecret);
    }

    /**
     * Creates the user agent using project properties in pom.xml
     * @return A {@link UserAgent} to facilitate the construction of a {{@link #SubredditIngestion()}}
     */
    private UserAgent createUserAgent() {
        // get project properties
        ProjectProperties projectProperties = this.getProjectProperties();
        String uniqueId = projectProperties.groupId + "." + projectProperties.artifactId;

        // get user name
        Credentials credentials = this.getCredentials();
        String username = credentials.getUsername();

        // instantiates the user agent
        return UserAgent.of("desktop", uniqueId, projectProperties.version, username);
    }

    /**
     * Retrieves subreddits as a combined {@link String} to facilitate the construction of a {@link #generateSubredditPaginator(int) generateSubredditPaginator}
     * @return The combined {@link String} of subreddits
     */
    private String getSubredditsAsCombinedString() {
        if(this.subreddits.isEmpty()) {
            return "";
        }
        else {
            return this.subreddits.stream().reduce("", (a,b) -> a + "+" + b).substring(1);
        }
    }

    /**
     * Sets the {@link SubredditPaginator} based on {@link #subreddits} using redditClient
     * @param limit The number of submissions to ingest
     */
    private void generateSubredditPaginator(int limit) {
        this.combinedSubreddits = getSubredditsAsCombinedString();
        if(this.combinedSubreddits.isEmpty()) {
            subredditPaginator = new SubredditPaginator(redditClient);
        }
        else {
            subredditPaginator = new SubredditPaginator(redditClient, this.combinedSubreddits);
        }
        subredditPaginator.setLimit(limit);
    }

    /**
     * Ingests submissions from {@link #subredditPaginator}
     */
    private void ingestSubmissions() {
        Listing<Submission> listing = this.getSubredditPaginator().next();
        this.submissions = ImmutableSet.copyOf(listing);
    }

    /**
     * Facilitates {@link #getProjectProperties() getProjectProperties} method
     */
    private class ProjectProperties {
        private String groupId;
        private String artifactId;
        private String version;

        ProjectProperties(String groupId, String artifactId, String version) {
            this.groupId = groupId;
            this.artifactId = artifactId;
            this.version = version;
        }
    }
}

