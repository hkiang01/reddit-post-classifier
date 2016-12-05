# reddit-post-classifier
Uses JRAW, juicer, Apache Tika, and Spark to classify Reddit posts by flair with a Unigram Language Model
## Getting started

### Import the project
- Clone the repo
- Start [IntelliJ](https://www.jetbrains.com/idea/)
- Open the repo directory in IntelliJ

### Adopt good Styleguide (optional)
- Follow instructions on [Installing the google styleguide settings in intellij and eclipse](https://github.com/HPI-Information-Systems/Metanome/wiki/Installing-the-google-styleguide-settings-in-intellij-and-eclipse)

### Credentials
- Create a file called `credentials.json` in `resources/credentials.json`

    ```
    {
      "username": "some_username",
      "password": "some_password",
      "clientId": "some_client_id",
      "clientSecret": "some_client_secret"
    }
    ```
    
- Obtain the above from Reddit's [OAuth2 page](https://github.com/reddit/reddit/wiki/OAuth2)

### Sample Results
- See [src/main/java/com/cs410dso/postclassifier/App.java](https://github.com/hkiang01/reddit-post-classifier/blob/master/src/main/java/com/cs410dso/postclassifier/App.java)
```
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


Classifier: NaiveBayesMultinomial
confusion matrix
25.0		8.0		1.0		3.0		
4.0		38.0		0.0		3.0		
10.0		6.0		22.0		2.0		
6.0		3.0		0.0		6.0		
error rate: 0.3357664233576642
mean absolute error: 0.1657695613940185
pct correct: 66.42335766423358
pct incorrect: 33.57664233576642
class 0
recall: 0.6756756756756757
precision: 0.5555555555555556
class 1
recall: 0.8444444444444444
precision: 0.6909090909090909
class 2
recall: 0.55
precision: 0.9565217391304348
class 3
recall: 0.4
precision: 0.42857142857142855


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
         
 ```