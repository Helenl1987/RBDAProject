# Directories and Files
* data_ingest
	* code and commands for data ingestion
* etl_code
	* code for data cleaning
* profiling_code
	* code for data profiling
* code_iterations
* screenshots
	* screenshots that show the analytic running
* analytics
	* analytics code
	* geo_feature.py
		* generate geographical features for businesses
	* geo_jensen.py
		* generate jensen feature for businesses
	* mobile_feature.py
		* generate mobility features for businesses
	* lrtest_geo.py
		* primary lr test to evaluate geographical features
	* lrtest_mobile.py
		* primary lr test to evaluate mobility features
	* TrendSpliting.java
		* generate time window for business trend data crawled from yelp
		* build and run
		
		```javac -classpath `yarn classpath` -d . TrendSpliting.java ```
		
		```jar -cvf trendSpliting.jar *.class ```
		
		```hadoop jar trendSpliting.jar TrendSpliting /user/jl10005/project/rating/crawl_result.tsv /user/jl10005/project/myout ```
		* results location
		
		```/user/jl10005/project/myout```
	* DO.java
		* review analysis (generate NLP features for reviews and do primary test to evaluate them)
		* build and run
		
		```javac -classpath /opt/cloudera/parcels/SPARK2-2.2.0.cloudera1-1.cdh5.12.0.p0.142354/lib/spark2/jars/*:`yarn classpath` DO.java ```
  		
  		```jar -cvf tryAnalytics.jar *.class```
  		
  		```spark2-submit --master yarn --deploy-mode cluster --class DO reviewAnalysis.jar ```
  		* results location
  		
  		```/user/jl10005/project/JOINDF```
  	* impala_record.sql
  		* primary table processing (selecting, filtering, joining) using impala
  		* build and run
  			* in impala shell
* /Readme.txt
	* readme file


# Input Data
* on dumbo: ```/user/jl10005/project```
* on dumbo: ```/user/zl2521/project```




