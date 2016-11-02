package com.cs410dso.postclassifier.ingestion;

import net.dean.jraw.RedditClient;
import net.dean.jraw.http.UserAgent;
import net.dean.jraw.http.oauth.Credentials;
import net.dean.jraw.http.oauth.OAuthData;
import net.dean.jraw.http.oauth.OAuthException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * This class provides an easy way to ingest the contents of a given subreddit.
 * This uses the {@link RedditClient} API
 * @see RedditClient
 */
public class SubredditIngestion {

    /** The subreddit to ingest */
    private String subreddit;

    /** The {@link RedditClient} used to facilitate the ingestion */
    private RedditClient redditClient;

    /** Used to fetch the project properties from pom.xml */
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

    /** Gets the credentials in order using {@link Credentials} */
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

    /** Creates the user agent using project properties in pom.xml */
    private UserAgent createUserAgent() {
        // get project properties
        ProjectProperties projectProperties = getProjectProperties();
        String uniqueId = projectProperties.groupId + "." + projectProperties.artifactId;

        // get user name
        Credentials credentials = getCredentials();
        String username = credentials.getUsername();

        // instantiates the user agent
        return UserAgent.of("desktop", uniqueId, projectProperties.version, username);
    }

    /** Instantiates a new SubredditIngestion for the given subreddit */
    public SubredditIngestion(String subreddit) {

        // class variable instantiation
        this.subreddit = subreddit;

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

    }

    /** Gets the username **/
    public String getUsername() {
        return getCredentials().getUsername();
    }

    /** Gets the subreddit */
    public String getSubreddit() {
        return subreddit;
    }

    /** Setse the subreddit */
    public void setSubreddit(String subreddit) {
        this.subreddit = subreddit;
    }

    public RedditClient getRedditClient() {
        return this.redditClient;
    }

    /** Facilitates {@link #getProjectProperties() getProjectProperties} method */
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
