# Directories and Files
* data_ingest
  * code and commands for data ingestion
* etl_code
  * code for data cleaning
* profiling_code
  * code for data profiling
* code_iterations
  * iterations
* screenshots
  * screenshots that show the combination model random forest and GBT spark jobs on three cities (Las Vegas, Toronto and Phoenix)
* analytics
  * analytics code
  * geo_feature.py
    * generate geographical features for businesses
    * Build and run
      *  run in yarn mode, change city name on line 14,17,108 can generate features on different cities and store in hdfs
      * ```spark2-submit --master yarn --deploy-mode cluster --executor-memory 10G --num-executors 10 geofeature.py```
  * mobile_feature.py
    * generate mobility features for businesses
    * Build and run
      * run in local mode (can deploy to HDFS and yarn, but didn't do it, since data is relatively small)
      * change checkin.txt file location on line 11, and features storage on 82 can generate features on different cities and store in local files
  * geo_fit_trend_lrtest.py
    * fit geo_features to business_trend to generate geo_trend_features and operate lrtest on geo_trend_features
    * Build and run
      * please run in pyspark (interactive mode)
  * mobile_fit_trend_lrtest.py
    * fit mobile_features to business_trend to generate mobile_trend_features and operate lrtest on mobile_trend_features
    * Build and run
      - please run in pyspark (interactive mode)
  * TrendSpliting.java
    * generate time window for business trend data crawled from yelp
    * build and run
    	* ```javac -classpath `yarn classpath` -d . TrendSpliting.java ```
    	* ```jar -cvf trendSpliting.jar *.class ```
    	* ```hadoop jar trendSpliting.jar TrendSpliting /user/jl10005/project/rating/crawl_result.tsv /user/jl10005/project/myout ```
    * results location
    	* ```/user/jl10005/project/myout```
  * DO.java
    * review analysis (generate NLP features for reviews and do primary test to evaluate them)
    * build and run
      * ```javac -classpath /opt/cloudera/parcels/SPARK2-2.2.0.cloudera1-1.cdh5.12.0.p0.142354/lib/spark2/jars/*:`yarn classpath` DO.java ```
      	* ```jar -cvf tryAnalytics.jar *.class```
      	* ```spark2-submit --master yarn --deploy-mode cluster --class DO reviewAnalysis.jar ```
      * results location
      	* may vary due to the invoking of different functions for various tasks, but all results will be under ```/user/jl10005/project/```
  * impala_record
    * primary table processing (selecting, filtering, joining) using impala
    * build and run
      * in impala shell
  * rf_train.py
    * random forest model trained  on combined features
    * build and run:
      * change city name on line 9,11,31can train and store model on different cities:
      * ```spark2-submit --deploy-mode cluster --executor-memory 20G --conf spark.yarn.executor.memoryoverhead=4096M rf_train.py```
  * gbt_train.py
    * gradient-boosted tree model trained on combined features
    * build and run:
      * change city name on line 15,17,37 can train and store model on different cities
      * ```spark2-submit --deploy-mode cluster --executor-memory 20G --conf spark.yarn.executor.memoryoverhead=4096M gbt_train.py```
  * xgb_train.py
    * extreme gradient-boosted tree model trained on combined features
    * build and run
      * please run in local intercative mode (python file)
      * input format in libsvm
* Readme.md
  * readme file


# Input Data
* on dumbo: ```/user/jl10005/project```
* on dumbo: ```/user/zl2521/project```




