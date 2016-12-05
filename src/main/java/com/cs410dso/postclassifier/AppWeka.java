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
                    System.out.print(matrix[outer][inner] + "\t\t");
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
                new NaiveBayesMultinomial()
        };

        String[] modelNames = {
                "J48",
                "NaiveBayes",
                "NaiveBayesMultinomial"
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
        /**
         TEST SET
         TEST SET
         TEST SET


         Classifier: J48
         confusion matrix
         21.0		8.0		5.0		3.0
         16.0		22.0		7.0		0.0
         5.0		4.0		29.0		2.0
         6.0		3.0		2.0		4.0
         error rate: 0.44525547445255476
         mean absolute error: 0.22430583406860777
         pct correct: 55.47445255474452
         pct incorrect: 44.52554744525548
         class 0
         recall: 0.5675675675675675
         precision: 0.4375
         class 1
         recall: 0.4888888888888889
         precision: 0.5945945945945946
         class 2
         recall: 0.725
         precision: 0.6744186046511628
         class 3
         recall: 0.26666666666666666
         precision: 0.4444444444444444


         Classifier: NaiveBayes
         confusion matrix
         21.0		11.0		3.0		2.0
         7.0		34.0		1.0		3.0
         4.0		6.0		28.0		2.0
         1.0		6.0		1.0		7.0
         error rate: 0.34306569343065696
         mean absolute error: 0.170092423398864
         pct correct: 65.69343065693431
         pct incorrect: 34.306569343065696
         class 0
         recall: 0.5675675675675675
         precision: 0.6363636363636364
         class 1
         recall: 0.7555555555555555
         precision: 0.5964912280701754
         class 2
         recall: 0.7
         precision: 0.8484848484848485
         class 3
         recall: 0.4666666666666667
         precision: 0.5


         Classifier: NaiveBayesMultinomialText
         confusion matrix
         0.0		0.0		37.0		0.0
         0.0		0.0		45.0		0.0
         0.0		0.0		40.0		0.0
         0.0		0.0		15.0		0.0
         error rate: 0.708029197080292
         mean absolute error: 0.3565642027346561
         pct correct: 29.197080291970803
         pct incorrect: 70.8029197080292
         class 0
         recall: 0.0
         precision: 0.0
         class 1
         recall: 0.0
         precision: 0.0
         class 2
         recall: 1.0
         precision: 0.291970802919708
         class 3
         recall: 0.0
         precision: 0.0


         END TEST SET
         END TEST SET
         END TEST SET




         ALL SET
         ALL SET
         ALL SET


         Classifier: J48
         confusion matrix
         98.0		10.0		5.0		4.0
         19.0		127.0		12.0		0.0
         7.0		8.0		158.0		3.0
         6.0		4.0		3.0		24.0
         error rate: 0.16598360655737704
         mean absolute error: 0.0922923985167838
         pct correct: 83.40163934426229
         pct incorrect: 16.598360655737704
         class 0
         recall: 0.8376068376068376
         precision: 0.7538461538461538
         class 1
         recall: 0.8037974683544303
         precision: 0.8523489932885906
         class 2
         recall: 0.8977272727272727
         precision: 0.8876404494382022
         class 3
         recall: 0.6486486486486487
         precision: 0.7741935483870968


         Classifier: NaiveBayes
         confusion matrix
         72.0		39.0		4.0		2.0
         12.0		137.0		5.0		4.0
         12.0		25.0		133.0		6.0
         2.0		12.0		1.0		22.0
         error rate: 0.2540983606557377
         mean absolute error: 0.1269454321190291
         pct correct: 74.59016393442623
         pct incorrect: 25.40983606557377
         class 0
         recall: 0.6153846153846154
         precision: 0.7346938775510204
         class 1
         recall: 0.8670886075949367
         precision: 0.6431924882629108
         class 2
         recall: 0.7556818181818182
         precision: 0.9300699300699301
         class 3
         recall: 0.5945945945945946
         precision: 0.6470588235294118


         Classifier: NaiveBayesMultinomial
         confusion matrix
         93.0		18.0		2.0		4.0
         6.0		148.0		0.0		4.0
         27.0		22.0		121.0		6.0
         10.0		4.0		0.0		23.0
         error rate: 0.21106557377049182
         mean absolute error: 0.10428454748746704
         pct correct: 78.89344262295081
         pct incorrect: 21.10655737704918
         class 0
         recall: 0.7948717948717948
         precision: 0.6838235294117647
         class 1
         recall: 0.9367088607594937
         precision: 0.7708333333333334
         class 2
         recall: 0.6875
         precision: 0.983739837398374
         class 3
         recall: 0.6216216216216216
         precision: 0.6216216216216216


         END ALL SET
         END ALL SET
         END ALL SET
         */
    }
