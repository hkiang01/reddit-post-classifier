package com.cs410dso.postclassifier;

import com.cs410dso.postclassifier.ingestion.FilteredSubredditIngestion;
import com.cs410dso.postclassifier.model.LocalSubredditFlairModel;
import scala.tools.cmd.gen.AnyVals;
import weka.classifiers.AbstractClassifier;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.bayes.NaiveBayesMultinomial;
import weka.classifiers.bayes.NaiveBayesMultinomialText;
import weka.classifiers.evaluation.AbstractEvaluationMetric;
import weka.classifiers.trees.J48;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.TextDirectoryLoader;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.StringToWordVector;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

    /**
     * AppWeka main for weka
     */
public class AppWeka {

    public static void printEval(Classifier[] models, String[] modelNames, int numClasses, Instances train, Instances test) throws Exception {
        for(int j = 0; j < models.length; j++) {

            Classifier classifier = models[j];
            classifier.buildClassifier(train);
            System.out.println("\n\nClassifier: " + modelNames[j]);

            // Collect every group of predictions for current model in a FastVector
            ArrayList predictions = new ArrayList();
            Evaluation evaluation = new Evaluation(train);
            classifier.buildClassifier(train);
            evaluation.evaluateModel(classifier, test);

            // http://weka.sourceforge.net/doc.dev/
            System.out.println("confusion matrix");
            double[][] matrix = evaluation.confusionMatrix();
            for(int outer = 0; outer < matrix.length; outer++) {
                for(int inner = 0; inner < matrix[outer].length; inner++) {
                    System.out.print(matrix[outer][inner] + "\t");
                }
                System.out.println();
            }
            System.out.println("error rate: " + evaluation.errorRate());
            System.out.println("mean absolute error: " + evaluation.meanAbsoluteError());
            System.out.println("pct correct: " + evaluation.pctCorrect());
            System.out.println("pct incorrect: " + evaluation.pctIncorrect());

            for(int i = 0; i < numClasses; i++) {
                System.out.println("class " + i);
                System.out.println("recall: " + evaluation.recall(i));
                System.out.println("precision: " + evaluation.precision(i));
            }
        }
    }

    // http://weka.wikispaces.com/file/view/TextCategorizationTest.java/82917279/TextCategorizationTest.java
    public static void main(String[] args) throws Exception {

        ArrayList<String> subreddits = new ArrayList<String>();
        subreddits.add("machinelearning");
        FilteredSubredditIngestion ingestion = new FilteredSubredditIngestion(subreddits, 1000);
        ingestion.saveSubmissionAndMetadataAboveThresholdAsJson();

        // scrape and in¡gest
        LocalSubredditFlairModel localSubredditFlairModel = new LocalSubredditFlairModel();
        localSubredditFlairModel.saveSubmissionsAsTxtUnderClassDirectories(); // save in weka friendly format

        // convert to Weka ARFF file
        final String WEKA_DIR_NAME = "weka";
        Path wekaBaseRealPath = null;
        try {
            wekaBaseRealPath = Paths.get(".").toRealPath();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String newRelativePathDirs = wekaBaseRealPath.toString() + "/" + WEKA_DIR_NAME;
        File wekaDir = new File(newRelativePathDirs);
        System.out.println("Setting directory to: " + wekaDir.toString());

        TextDirectoryLoader textDirectoryLoader = new TextDirectoryLoader();
        try {
            textDirectoryLoader.setDirectory(wekaDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Directory set to:  " + textDirectoryLoader.getDirectory().toString());


        // get weka instances
        Instances dataRaw = textDirectoryLoader.getDataSet();

        //System.out.println("\n\nImported data:\n\n" + dataRaw);

        // apply the StringToWordVector
        // (see the source code of setOptions(String[]) method of the filter
        // if you want to know which command-line option corresponds to which
        // bean property)
        StringToWordVector filter = new StringToWordVector();
        filter.setInputFormat(dataRaw);
        Instances dataFiltered = Filter.useFilter(dataRaw, filter);
//        System.out.println("\n\nFiltered data:\n\n" + dataFiltered);

        // https://stackoverflow.com/questions/14682057/java-weka-how-to-specify-split-percentage
        // split into test and train
        double percent = 72.0;
        long randomSeed = 0;
        int trainSize = (int) Math.round(dataFiltered.numInstances() * percent / 100);
        int testSize = dataFiltered.numInstances() - trainSize;
        dataFiltered.randomize(new Random(randomSeed));

        Instances train = new Instances(dataFiltered, 0, trainSize);
        Instances test = new Instances(dataFiltered, trainSize, testSize);
        Instances all = dataFiltered;
        // list of classifiers: http://weka.sourceforge.net/doc.dev/weka/classifiers/Classifier.html

        System.out.println("Number of train instances: " + train.numInstances());
        System.out.println("Number of test instances: " + test.numInstances());

        // http://www.cs.umb.edu/~ding/history/480_697_spring_2013/homework/WekaTest.java
        Classifier[] models = {
                new J48(),
                new NaiveBayes(),
                new NaiveBayesMultinomialText()
        };

        String[] modelNames = {
                "J48",
                "NaiveBayes",
                "NaiveBayesMultinomialText"
        };

        System.out.println("\n\nTEST SET");
        System.out.println("TEST SET");
        System.out.println("TEST SET");
        printEval(models, modelNames, dataFiltered.numClasses(), train, test);
        System.out.println("\n\nEND TEST SET");
        System.out.println("END TEST SET");
        System.out.println("END TEST SET\n\n");

        System.out.println("\n\nALL SET");
        System.out.println("ALL SET");
        System.out.println("ALL SET");
        printEval(models, modelNames, dataFiltered.numClasses(), train, all);
        System.out.println("\n\nEND ALL SET");
        System.out.println("END ALL SET");
        System.out.println("END ALL SET\n\n");

    }
}
