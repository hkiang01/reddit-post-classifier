package com.cs410dso.postclassifier.ingestion;

import com.cs410dso.postclassifier.util.SubmissionAndTextWrapper;
import com.google.common.base.Function;
import com.google.common.collect.*;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import com.fasterxml.jackson.databind.ObjectMapper;

import net.dean.jraw.models.Flair;
import net.dean.jraw.models.Submission;

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.pdf.PDFParser;
import org.apache.tika.sax.BodyContentHandler;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.mortbay.util.ajax.JSON;
import spire.math.algebraic.Sub;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import static java.util.Arrays.asList;


import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

/**
 * Facilitates the ingestion of {@link net.dean.jraw.models.Submission} from a {@link Collection} of subreddits using a set of custom filtering rules.
 */
public class FilteredSubredditIngestion extends SubredditIngestion {

    /**
     * The URL used for Juicer API
     */
    private static final String JUICER_PREPEND_URL = "https://juicer.herokuapp.com/api/article?url=";

    public static final String JSON_PATH = "./data.json";

    public static final String WEKA_DIR_NAME = "weka";

    private static final int NUM_CPU_CORES = 4;

    /**
     * The minimum length of a "valid" text file
     */
    private static final int TEXT_THRESHOLD = 150;

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
        ImmutableCollection<Submission> unBucketed =  this.getSubmissions();
        Function<Submission, Boolean> isStickiedFunction = new Function<Submission, Boolean>() {
            public Boolean apply(Submission submission) {
                return submission.isStickied();
            }
        };
        return Multimaps.index(unBucketed, isStickiedFunction);
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
        System.out.println("retrieved " + submissions.size() + " submissions");

//        Collection<String> lowercaseSelfDomains = selfDomains.stream()
//                .map(String::toLowerCase)
//                .collect(Collectors.toCollection(TreeSet::new));
        Collection<String> selfDomainsCollection = this.getSubredditSelfDomains();
        ArrayList<String> selfDomainsList = new ArrayList<>();
        Iterator<String> it = selfDomainsCollection.iterator();
        while(it.hasNext()) {
            selfDomainsList.add(it.next().toLowerCase());
        }
        final ArrayList<String> lowercaseSelfDomains = selfDomainsList;

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
     * @return a {@link Collection} containing {@link java.util.AbstractMap.SimpleEntry} of {@link Submission}s and {@link UrlAuthorFlairMethodText}s
     */
    public Collection<AbstractMap.SimpleEntry<Submission, UrlAuthorFlairMethodText>> getSubmissionsAndMetadata() {
        ImmutableListMultimap<Boolean, Submission> submissions = getSubmissionsBySelf();
        System.out.println("getting submission and metadata for " + submissions.size() + " submissions");

//        return submissions.entries().parallelStream().map(e -> { // parallel streams
//            Submission curSubmission = e.getValue();
//            if (e.getKey()) { // if the entry's domain is from self.subreddit
//                UrlAuthorFlairMethodText submissionTextFlair = new UrlAuthorFlairMethodText(curSubmission, "JRAW's getSelftext", curSubmission.getSelftext());
//                return new AbstractMap.SimpleEntry<Submission, UrlAuthorFlairMethodText>(curSubmission, submissionTextFlair);
//            } else {
//                String url = curSubmission.getUrl();
//                String text = getSubmissionArticleTextViajuicer(url);
//                String method = "juicer";
//                if(text.length() < TEXT_THRESHOLD) {
//                    text = getSubmissionArticleViaTikaAutoDetectParser(url);
//                    method = "Apache Tika Auto-Detect Parser";
//                }
//                if(text.length() < TEXT_THRESHOLD && url.contains("pdf")) {
//                    text = getSubmissionArticleViaTikaPDFParser(url);
//                    method = "Apache Tika PDF Parser";
//                }
//                String textWithoutWhitespace = text.replaceAll("\\s+", " "); // https://stackoverflow.com/questions/18870395/how-to-remove-spaces-in-between-the-string
//                UrlAuthorFlairMethodText submissionTextFlair = new UrlAuthorFlairMethodText(curSubmission, method, textWithoutWhitespace);
//                return new AbstractMap.SimpleEntry<Submission, UrlAuthorFlairMethodText>(curSubmission, submissionTextFlair);
//            }
//        }).collect(Collectors.toCollection(HashSet::new)); // hashset for constant time

        // the submissions to processs
        final ImmutableCollection<Map.Entry<Boolean, Submission>> entries = submissions.entries();

        // https://stackoverflow.com/questions/2016083/what-is-the-easiest-way-to-parallelize-a-task-in-java
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_CPU_CORES);
        List<Callable<SubmissionAndTextWrapper>> tasks = new ArrayList<Callable<SubmissionAndTextWrapper>>();

        // the parallel call
        for(final Map.Entry<Boolean, Submission> curr : entries) {
            Callable<SubmissionAndTextWrapper> c = new Callable<SubmissionAndTextWrapper>() {
                @Override
                public SubmissionAndTextWrapper call() throws Exception {
                    Submission submission = curr.getValue();
                    boolean isSelf = curr.getKey();
                    return extractTextFromSubmission(submission, isSelf);
                }
            };
            tasks.add(c);
        }

        // the parallel collection (extract the result from the wrapper class)
        ArrayList<AbstractMap.SimpleEntry<Submission, UrlAuthorFlairMethodText>> retval = new ArrayList<>();
        try {
            List<Future<SubmissionAndTextWrapper>> results = executorService.invokeAll(tasks);
            for(Future<SubmissionAndTextWrapper> future : results) {
                SubmissionAndTextWrapper res = future.get();
                final AbstractMap.SimpleEntry<Submission, UrlAuthorFlairMethodText> item = res.item;
                retval.add(item);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return retval;
    }

    /**
     * Calls extractos to extract text of submission
     * @param submission The submission to extract text from
     * @param isSelf Whether or not the submission was a post to a self domain
     * @return The Submission and its associated url, author, flair, method of extraction, and text
     */
    private SubmissionAndTextWrapper extractTextFromSubmission(Submission submission, boolean isSelf) {
        if (isSelf) { // if the entry's domain is from self.subreddit
            UrlAuthorFlairMethodText submissionTextFlair = new UrlAuthorFlairMethodText(submission, "JRAW's getSelftext", submission.getSelftext());
            return new SubmissionAndTextWrapper(new AbstractMap.SimpleEntry<Submission, UrlAuthorFlairMethodText>(submission, submissionTextFlair));
        } else {
            String url = submission.getUrl();
            String text = getSubmissionArticleTextViajuicer(url);
            String method = "juicer";
            if(text.length() < TEXT_THRESHOLD) {
                text = getSubmissionArticleViaTikaAutoDetectParser(url);
                method = "Apache Tika Auto-Detect Parser";
            }
            if(text.length() < TEXT_THRESHOLD && url.contains("pdf")) {
                text = getSubmissionArticleViaTikaPDFParser(url);
                method = "Apache Tika PDF Parser";
            }
            String textWithoutWhitespace = text.replaceAll("\\s+", " "); // https://stackoverflow.com/questions/18870395/how-to-remove-spaces-in-between-the-string
            UrlAuthorFlairMethodText submissionTextFlair = new UrlAuthorFlairMethodText(submission, method, textWithoutWhitespace);
            return new SubmissionAndTextWrapper(new AbstractMap.SimpleEntry<Submission, UrlAuthorFlairMethodText>(submission, submissionTextFlair));
        }
    }

    /**
     * Uses Apache Tika's PDF Parser to parse text from a URL
     * @param urlString the URL to plug into Apache Tika's Auto-Detect Parser
     * @return the body result of the juicer API call
     * @see <a href="https://tika.apache.org/1.14/examples.html#Parsing_using_the_Auto-Detect_Parser">Apache Tika's Auto-Detect Parser</a>
     * WARNING: Does not work for all sites and document types, e.g., PDFs
     */
    private String getSubmissionArticleViaTikaPDFParser(String urlString) {
        // https://tika.apache.org/1.14/examples.html#Parsing_using_the_Auto-Detect_Parser
        return tikaParserHelper(urlString, new PDFParser(), new BodyContentHandler(), new Metadata(), new ParseContext());
    }

    /**
     * Uses Apache Tika's Auto-Detect Parser to parse text from a URL
     * @param urlString the URL to plug into Apache Tika's Auto-Detect Parser
     * @return the body result of the juicer API call
     * @see <a href="https://tika.apache.org/1.14/examples.html#Parsing_using_the_Auto-Detect_Parser">Apache Tika's Auto-Detect Parser</a>
     * WARNING: Does not work for all sites and document types, e.g., PDFs
     */
    private String getSubmissionArticleViaTikaAutoDetectParser(String urlString) {
        return tikaParserHelper(urlString, new AutoDetectParser(), new BodyContentHandler(), new Metadata(), new ParseContext());
    }

    /**
     * A helper for using Apache Tika's Parser API
     * @param urlString the url to parse
     * @param parser the {@link AbstractParser}
     * @param bodyContentHandler the {@link BodyContentHandler}
     * @param metadata the {@link Metadata}
     * @return the resultant parsed string
     * @see  <a href="https://tika.apache.org/1.14/examples.html#Parsing_using_the_Auto-Detect_Parser">https://tika.apache.org/1.14/examples.html#Parsing_using_the_Auto-Detect_Parser</a>
     */
    private String tikaParserHelper(String urlString, Parser parser, BodyContentHandler bodyContentHandler, Metadata metadata, ParseContext parseContext) {
        // https://tika.apache.org/1.14/examples.html#Parsing_using_the_Auto-Detect_Parser
        try{
            // https://docs.oracle.com/javase/tutorial/networking/urls/readingURL.html
            URL url = new URL(urlString);
            InputStream inputStream = url.openStream();
            parser.parse(inputStream, bodyContentHandler, metadata, parseContext);
            return bodyContentHandler.toString();

        } catch (Exception e){
            // what if the protocol in the URL messed up http vs https?
            // https://stackoverflow.com/questions/1171513/how-to-change-only-the-protocol-part-of-a-java-net-url-object
            try {
                URL url = new URL(urlString);
                String protocol = url.getProtocol();
                if (protocol.equals("http")) {
                    url = new URL("https", url.getHost(), url.getPort(), url.getFile());
                }
                else if(protocol.equals("https")) {
                    url = new URL("http", url.getHost(), url.getPort(), url.getFile());
                }
                InputStream inputStream = url.openStream();
                parser.parse(inputStream, bodyContentHandler, metadata, parseContext);
                return bodyContentHandler.toString();
            } catch (Exception eInner) {
                eInner.printStackTrace();
            }
            e.printStackTrace();
            return "";
        }
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
            // http://howtodoinjava.com/core-java/io/how-to-read-data-from-inputstream-into-string-in-java/
            URL url = new URL(JUICER_PREPEND_URL + urlString);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader((url).openStream()));
            StringBuilder stringBuilder = new StringBuilder();
            String line;
            while((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line);
            }
            String result = stringBuilder.toString();
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
     * Get a collection of text and flair whose text entries are above TEXT_THRESHOLD
     * @return a {@link Collection} containing {@link JSONObject} containing both the {@link Submission} and its associated metadata (url, author, flair, method, text)
     */
    public Collection<JSONObject> getSubmissionAndMetadataAboveThresholdAsJson() {
        Collection<AbstractMap.SimpleEntry<Submission, UrlAuthorFlairMethodText>> entries = this.getSubmissionsAndMetadata();
        System.out.println("filtering " + entries.size() + " entries");
//        return entries.parallelStream()
//                .filter(e -> e.getValue().text.length() >= TEXT_THRESHOLD)
//                .map(e -> {
//                    Submission submission = e.getKey();
//                    ObjectMapper mapper = new ObjectMapper();
//                    Map<String, Object> result = mapper.convertValue(submission.getDataNode(), Map.class);
//                    JSONObject submissionJsonObject = new JSONObject(result);
//                    JSONObject jsonObject = e.getValue().toJSONObject();
//                    jsonObject.put("submission", submissionJsonObject);
//                    return jsonObject;
//                })
//                .collect(Collectors.toCollection(HashSet::new));
        ArrayList<JSONObject> filteredEntries = new ArrayList<JSONObject>();
        for (Iterator<AbstractMap.SimpleEntry<Submission, UrlAuthorFlairMethodText>> it = entries.iterator(); it.hasNext(); ) {
            AbstractMap.SimpleEntry<Submission, UrlAuthorFlairMethodText> e = it.next();
            if (e.getValue().text.length() >= TEXT_THRESHOLD) {
                Submission submission = e.getKey();
                ObjectMapper mapper = new ObjectMapper();
                Map<String, Object> result = mapper.convertValue(submission.getDataNode(), Map.class);
                JSONObject submissionJsonObject = new JSONObject(result);
                JSONObject jsonObject = e.getValue().toJSONObject();
                jsonObject.put("submission", submissionJsonObject);
                filteredEntries.add(jsonObject);
            }
        }
        return filteredEntries;
    }

    /**
     * Gets author and created from jsonObject and joins them with an underscore to get [author]_[created]
     * @param jsonObject the {@link JSONObject} containing a {@link Submission} and associated metadata
     * @return a {@link String} with the format [author]_[created]
     */
    private String getAuthorUnderscoreCreatedUniqueKey(JSONObject jsonObject) {
//        String author = jsonObject.getOrDefault("author", "").toString();
//        String created = jsonObject.getOrDefault("created", "").toString();

        String author;
        try {
            author = jsonObject.get("author").toString();
        } catch (Exception e) {
            author = "";
        }

        String created;
        try {
            created = jsonObject.get("created").toString();
        } catch (Exception e) {
            created = "";
        }

        return author.concat("_").concat(created);
    }

    /**
     * Similar to etSubmissionAndMetadataAboveThreshold but only with a key, text, and flair per entry
     * @return the collection of {@link JSONObject} elements containing a key, some text, and flair
     */
    public Collection<JSONObject> getSubmissionAndMetadataAboveThresholdWithFilteredFields() {
        Collection<JSONObject> posts = this.getSubmissionAndMetadataAboveThresholdAsJson();

        //        return posts.parallelStream().map(e -> {
//            JSONObject jsonObject = new JSONObject();
//            String key = getAuthorUnderscoreCreatedUniqueKey(e);
//            jsonObject.put("author", e.get("author"));
//            jsonObject.put("created", e.get("created"));
//            jsonObject.put("text", e.get("text"));
//            jsonObject.put("flair", e.get("flair"));
//            return jsonObject;
//        }).collect(Collectors.toCollection(HashSet::new));

        ArrayList<JSONObject> retval = new ArrayList<>();
        for (Iterator<JSONObject> it = posts.iterator(); it.hasNext(); ) {
          JSONObject e = it.next();
            JSONObject jsonObject = new JSONObject();
            String key = getAuthorUnderscoreCreatedUniqueKey(e);
            jsonObject.put("author", e.get("author"));
            jsonObject.put("created", e.get("created"));
            jsonObject.put("text", e.get("text"));
            jsonObject.put("flair", e.get("flair"));
            retval.add(jsonObject);
        }
        return retval;

    }

    /**
     * Writes posts with [author]_[created] as key and submission and associated metadata as value
     * in a single json file in JSON_PATH
     */
    public void saveSubmissionAndMetadataAboveThresholdAsJson() {
        Collection<JSONObject> posts = this.getSubmissionAndMetadataAboveThresholdWithFilteredFields();
        System.out.println("Saving " + posts.size() + " posts");

        Path p = Paths.get(JSON_PATH);
//        String combinedPosts = posts.stream().map(e -> e.toString())
//        .reduce("", (accumulator, e) -> {
//            // accumulate them all into a single String
//            return accumulator + "\n" + e;
//        }).substring(1); // case where accumulator is '\n'
        String combinedPosts = "";
        for(Iterator<JSONObject> it = posts.iterator(); it.hasNext();) {
            String e = it.next().toString();
            combinedPosts = combinedPosts + e + "\n";
        }

        byte data[] = combinedPosts.getBytes();
        try (OutputStream out = new BufferedOutputStream(
                Files.newOutputStream(p, CREATE, APPEND))) {
            out.write(data, 0, data.length);
        } catch (IOException x) {
            x.printStackTrace();
        }
    }


    /**
     * Gets the path to JSON_PATH readable by Spark using Spark 2.0.2's spark.read.json([the path])
     * @return the path
     */
    public String getSparkDataPath() {
        String path = "";
        try {
            path = "file://" + Paths.get(JSON_PATH).toRealPath().toString();
        } catch (IOException e){
            e.printStackTrace();
        }
        return path;
    }

    /**
     * Checks to see whether JSON_DATA file is empty
     * @return whether or not it's empty
     */
    public boolean isDataEmpty() {
        File file = new File(JSON_PATH);
        return !file.exists() || file.length() == 0;
    }

    /**
     * Used to store text and flair of a {@link Submission}
     */
    public class UrlAuthorFlairMethodText {

        /**
         * The url
         */
        private String url;

        /**
         * The author
         */
        private String author;

        /**
         * The {@link Flair}
         */
        private Flair flair;

        /**
         * The method
         */
        private String method;

        /**
         * The text
         */
        private String text;

        /**
         * The date the Submission was created
         */
        private Date created;

        /**
         * Parameterized constructor
         * @param submission A {@link Submission}
         * @param method The method used to extract the text
         * @param text A {@link String} of text
         */

        UrlAuthorFlairMethodText(Submission submission, String method, String text) {
            this.url = submission.getUrl();
            this.author = submission.getAuthor();
            this.flair = submission.getSubmissionFlair();
            this.method = method;
            this.text = text;
            this.created = submission.getCreated();
        }

        /**
         * A {@link String} representation of a {@link UrlAuthorFlairMethodText}
         * @return The {@link String} representation
         */
        public String toString() {
            return "\n================================================================================\n" +
                    "url: " + this.url + "\n" +
                    "author: " + this.author + "\n" +
                    "flair: " + flair.getCssClass() + "\t" + flair.getText() + "\n" +
                    "method: " + this.method + "\n" +
                    "text length: " + text.length() + " \n" +
                    "text: " + text + "\n" +
                    "================================================================================\n";
        }

        /**
         * Converts class instance into a {@link Map}
         * @return A {@link Map} with url, flair, method, text length, and text of an instance.
         */
        public Map<String, String> toMap() {
            Map<String, String> myMap = new HashMap<>();
            myMap.put("url", this.url);
            myMap.put("author", this.author);
            myMap.put("created", this.created.toString());
            myMap.put("flair", flair.getCssClass());
            myMap.put("method", this.method);
            myMap.put("text length", Integer.toString(this.text.length()));
            myMap.put("text", this.text);
            return myMap;
        }

        /**
         * Converts class instance into a {@link JSONObject}
         * @return A {@link JSONObject} with url, flair, method, text length, and text of an instance.
         */
        public JSONObject toJSONObject() {
            return new JSONObject(this.toMap());
        }
    }

}
