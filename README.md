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
- See [src/app/App.java](https://github.com/hkiang01/reddit-post-classifier/blob/master/src/main/java/com/cs410dso/postclassifier/App.java)
```
        /*
        root
         |-- label: double (nullable = true)
         |-- created: string (nullable = true)
         |-- author: string (nullable = true)
         |-- text: string (nullable = true)
         |-- prediction: double (nullable = true)
         |-- score: double (nullable = true)
         |-- max_score: double (nullable = true)

        +-----+--------------------+-------------------+--------------------+----------+-------------------+-------------------+
        |label|             created|             author|                text|prediction|              score|          max_score|
        +-----+--------------------+-------------------+--------------------+----------+-------------------+-------------------+
        |  1.0|Mon Nov 14 10:15:...|         Mandrathax|This is a place t...|       1.0| -47.18677502636149| -47.18677502636149|
        |  0.0|Tue Oct 11 17:22:...|     rmltestaccount| LI YAO et al.: O...|       0.0| -647.1849626287669| -647.1849626287669|
        |  1.0|Mon Nov 28 00:08:...|darkconfidantislife|Hey there guys,

        ...|       1.0| -32.70491276362627| -32.70491276362627|
        |  2.0|Mon Nov 28 10:21:...|             dtraxl|DeepGraph is a sc...|       0.0| -74.34824862903571| -74.34824862903571|
        |  0.0|Mon Nov 28 17:45:...|          omoindrot|In past blog post...|       0.0|  -534.707779547275|  -534.707779547275|
        |  0.0|Fri Nov 11 12:03:...|       downtownslim| Under review as ...|       0.0|  -647.184962628767|  -647.184962628767|
        |  1.0|Sat Nov 26 08:09:...|              cptai|I am reading the ...|       1.0| -20.17689598547549| -20.17689598547549|
        |  1.0|Thu Nov 17 14:57:...|        bronzestick|In several applic...|       1.0| -42.94189034717664| -42.94189034717664|
        |  0.0|Tue Oct 18 10:21:...|             tuan3w| Cornell Universi...|       0.0| -79.89321736309498| -79.89321736309498|
        |  1.0|Sat Nov 05 04:15:...|        wjbianjason|To be specific,wh...|       1.0|-12.573424782221249|-12.573424782221249|
        |  1.0|Thu Oct 13 16:34:...|           Pieranha|The Densely Conne...|       1.0| -36.21456066940183| -36.21456066940183|
        |  1.0|Wed Oct 19 20:12:...| frustrated_lunatic|In Liu CiXin’s no...|       1.0| -53.00182763938054| -53.00182763938054|
        |  2.0|Mon Nov 28 19:35:...|           Weihua99| Skip to content ...|       0.0| -48.11865459561534| -48.11865459561534|
        |  0.0|Mon Nov 14 13:03:...|          jhartford|For details and a...|       0.0| -8.039219933126056| -8.039219933126056|
        |  1.0|Tue Nov 08 10:52:...|           huyhcmut|How can I train a...|       1.0|-12.573424782221249|-12.573424782221249|
        |  2.0|Tue Oct 25 14:57:...|      shagunsodhani| Skip to content ...|       0.0| -85.83494706485156| -85.83494706485156|
        |  3.0|Wed Nov 16 16:30:...|             clbam8|Here at AYLIEN we...|       0.0|-133.20035411396975|-133.20035411396975|
        |  1.0|Tue Oct 25 20:49:...|           jayjaymz|Hello there. I'm ...|       1.0| -35.05147929983538| -35.05147929983538|
        |  1.0|Tue Nov 29 07:05:...|             Kiuhnm|I'm reading Mansi...|       1.0|-28.675704273027858|-28.675704273027858|
        |  2.0|Tue Nov 29 23:33:...|        longinglove|Can we segment un...|       0.0| -79.35532502384983| -79.35532502384983|
        +-----+--------------------+-------------------+--------------------+----------+-------------------+-------------------+
        only showing top 20 rows
         */
 ```
