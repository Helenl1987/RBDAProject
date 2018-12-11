//import org.apache.spark.SparkConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.*;
import org.apache.spark.ml.clustering.DistributedLDAModel;
import org.apache.spark.ml.clustering.LDA;
import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.ml.clustering.LocalLDAModel;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.*;
//import org.apache.spark.mllib.classification.LogisticRegressionModel;
//import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
//import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
//import org.apache.spark.mllib.feature.ChiSqSelector;
import org.apache.spark.mllib.feature.Word2Vec;
import org.apache.spark.mllib.feature.Word2VecModel;
//import org.apache.spark.sql.Dataset;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import scala.Array;
import scala.Tuple2;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.*;
import java.util.regex.Pattern;

public class DO {
    public static void main(String[] args) throws Exception{

//        JavaWordCount javaWordCount = new JavaWordCount();
//        javaWordCount.wordCount(new String[]{fpath});

        ReviewAnalysis reviewAnalysis = new ReviewAnalysis();
        reviewAnalysis.preProcessing(null);
//        reviewAnalysis.reviewPredict(null);
//        reviewAnalysis.runSeperately(null);
    }
}



class ReviewAnalysis {
    static final String prepath_local = "/Users/Helen/Downloads/18Fall/RBDA/RBDAProject/yelp_dataset/ProjectRecord/";
    static final String prepath_dumbo = "/user/jl10005/project/";
    static final String prepath_dumbo_local = "/home/jl10005/project/TryAnalytics/";

    static final String prepath = prepath_local;
//    static final String prepath = prepath_dumbo;
    static final String cityname = "phoenix";
//    static final String cityname = "toronto";
//    static final String cityname = "lasvegas";


    public void runSeperately(String[] args) throws Exception {
        String reviewpath = prepath + "review_version1.txt";
        String trendpath = prepath + "crawling_withtrend.txt";
        SparkSession spark = SparkSession
                .builder()
                .config("spark.executor.memory", "4g")
                .master("local")
                .appName("wordToVecPreProcessing")
                .getOrCreate();

        JavaRDD<String> reviewlines = spark
                .read()
                .textFile(reviewpath)
                .limit(500)
                .javaRDD();
        JavaRDD<String> trendlines = spark
                .read()
                .textFile(trendpath)
//                .limit(8)
                .javaRDD();
        JavaPairRDD<String, Double> trendMonthlyMapping = getTrendMonthlyMapping(trendlines);
        Map<String, Double> trendMonthlyMap = trendMonthlyMapping.collectAsMap();
        JavaRDD<Row> data = reviewlines.map(s -> {
            String[] splitstring = s.split("\t");
            // schema: 0->review_id, 1->user_id, 2->business_id, 3->starts, 4->date, 5->text, 6->useful, 7->funny, 8->cool
            String cleanedDate = cleanDate(splitstring[4]);
            String key = String.format("%s,%s", splitstring[2], cleanedDate);
            if(trendMonthlyMap.containsKey(key)) {
                double label = normalize(trendMonthlyMap.get(key));
                label = label > 10 ? 1 : 0;
                return RowFactory.create(label, Arrays.asList(s.split("\t")[5].trim().split(" |\t|\n|\\.")));
            }
            else
                return null;
        }).filter(s -> {return s != null;});
        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });
        Dataset<Row> df = spark.createDataFrame(
                data.collect(),
                schema);
        df.write().json
                (String.format("%sDF", prepath));

//        Dataset<Row> df = spark.read().json(String.format("%sDF", prepath));
//        df.show(false);
        CountVectorizerModel countVectorizerModel = new CountVectorizer()
                .setInputCol("text")
                .setOutputCol("feature")
//                .setMinDF(1)
                .setMinDF(1000)
//                .setVocabSize(5)
                .fit(df);
        Dataset<Row> cvdf = countVectorizerModel
                .transform(df)
                .drop("text");
        spark.stop();
        return;
    }

    public JavaRDD<String> getReviewlines(SparkSession spark) {
        //        String reviewpath = prepath + "review_version1.txt";
//        String reviewpath = prepath + "review_version1.2";
//        String reviewpath = prepath + "review_version2.1.txt";
//        String reviewpath = prepath + "review_version2.2";

//        String reviewpath = prepath + "review_toronto_version1.txt";
//        String reviewpath = prepath + "review_phoenix_version1.txt";
//        String reviewpath = prepath + "review_toronto_version3";
//        String reviewpath = prepath + "review_phoenix_version3";
//        String reviewpath = prepath + "trend_review_toronto.txt";
//        String reviewpath = prepath + "trend_review_phoenix.txt";
        String reviewpath = prepath + "review_" + cityname + "_version3";


        JavaRDD<String> reviewlines = spark
                .read()
                .textFile(reviewpath)
//                .limit(500)
//                .limit(100000)
                .javaRDD();

        System.out.println(String.format("reviewlines = %d", reviewlines.count()));
        return reviewlines;
    }

    public JavaRDD<String> getTrendlines(SparkSession spark) {
        String trendpath = prepath + "crawling_withtrend2.txt";

        JavaRDD<String> trendlines = spark
                .read()
                .textFile(trendpath)
//                .limit(8)
                .javaRDD();

        System.out.println(String.format("trendlines = %d", trendlines.count()));
        return trendlines;
    }

    public void preProcessing(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
//                .config("spark.executor.memory", "4g")
                .master("local")
                .appName("RBDAProject")
                .getOrCreate();

//        reviewProcess(spark);
        bagOfWordModel(spark);
//        ldaModel(spark);
//        aspectSentimentModel(spark);
//        trendPreprocessing(spark);
//        GBTModel(spark);
//        cleanReview(spark);
//        reviewFilter(spark);

        spark.stop();
        return;
    }

    public Word2VecModel getWord2VecModel(SparkSession spark) {
        JavaRDD<String> reviewlines = getReviewlines(spark);

        Word2Vec word2Vec = new Word2Vec()
                .setMinCount(0)
                .setVectorSize(128);

        JavaRDD<List<String>> reviews = reviewlines.map(s -> Arrays.asList(s.split("\t")[5].trim().split(" |\t|\n|\\.")));

        Word2VecModel word2VecModel = word2Vec.fit(reviews);

//        Map<String, float[]> vectors = scala
//                .collection
//                .JavaConverters
//                .mapAsJavaMapConverter(word2VecModel.getVectors())
//                .asJava();
//
//        System.out.println(String.format("%d\t%d", vectors.size(), vectors.get(vectors.keySet().iterator().next()).length));

        return word2VecModel;
    }

    public void generateLabeledPointForWord(SparkSession spark) {
        JavaRDD<String> reviewlines = getReviewlines(spark);
        JavaRDD<String> trendlines = getTrendlines(spark);
        Word2VecModel word2VecModel = getWord2VecModel(spark);

        String foutpath = prepath + "LabeledPointForWord";
        JavaPairRDD<String, Double> trendMonthlyMapping = getTrendMonthlyMapping(trendlines);

        Map<String, Double> trendMonthlyMap = trendMonthlyMapping.collectAsMap();

        JavaPairRDD<Vector, Double> wordLabelMapping = reviewlines.flatMapToPair(s -> {
            String[] splitstring = s.split("\t");
            // schema: 0->review_id, 1->user_id, 2->business_id, 3->starts, 4->date, 5->text, 6->useful, 7->funny, 8->cool
            String cleanedDate = cleanDate(splitstring[4]);
            String key = String.format("%s,%s", splitstring[2], cleanedDate);
            String[] wordset = splitstring[5].trim().split(" |\t|\n|\\.");
            List<Tuple2<Vector, Double>> list = new ArrayList<Tuple2<Vector, Double>>();
            if(trendMonthlyMap.containsKey(key)) {
                double label = normalize(trendMonthlyMap.get(key));
                for (String str : wordset) {
                    list.add(new Tuple2<>(word2VecModel.transform(str), label));
                }
            }
            return list.iterator();
        })
                .groupByKey().mapToPair(s -> {
                    double sum = 0.0;
                    double cnt = 0.0;
                    for(double d: s._2()) {
                        sum += d;
                        cnt += 1;
                    }
                    double avr = (sum/cnt)/0.5*0.5;
                    return new Tuple2<>(s._1(), avr);
                });

        JavaRDD<LabeledPoint> wordLP = wordLabelMapping
                .map(s -> new LabeledPoint(s._2(), s._1()));

        wordLP.saveAsTextFile(foutpath);
        return;
    }

    public Dataset<Row> generateLabeledPointForReview(SparkSession spark) {
        JavaRDD<String> reviewlines = getReviewlines(spark);
        JavaRDD<String> trendlines = getTrendlines(spark);

        JavaPairRDD<String, Double> trendMonthlyMapping = getTrendMonthlyMapping(trendlines);
        Map<String, Double> trendMonthlyMap = trendMonthlyMapping.collectAsMap();

        JavaRDD<Row> data = reviewlines.map(s -> {
            String[] splitstring = s.split("\t");
            // schema: 0->review_id, 1->user_id, 2->business_id, 3->starts, 4->date, 5->text, 6->useful, 7->funny, 8->cool
            String cleanedDate = cleanDate(splitstring[4]);
            String key = String.format("%s,%s", splitstring[2], cleanedDate);
            if(trendMonthlyMap.containsKey(key)) {
                double label = normalize(trendMonthlyMap.get(key));
//                label = label > 10 ? 1 : 0;
                return RowFactory.create(label, Arrays.asList(s.split("\t")[5].split(" |\t|\n|\\.")));
            }
            else
                return null;
        }).filter(s -> {return s != null;});

        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });
        Dataset<Row> df = spark.createDataFrame(
                data.collect(),
                schema);
//        df.show();
        return df;
    }

    public void generateLabeledPointForReviewMonthly(SparkSession spark) {
        JavaRDD<String> trendlines = getTrendlines(spark);

        String foutpath = prepath + "LabeledPointForReviewMonthly";

        JavaPairRDD<String, Double> trendMonthlyMapping = getTrendMonthlyMapping(trendlines);

        Map<String, Double> trendMonthlyMap = trendMonthlyMapping.collectAsMap();

        JavaPairRDD<String, List<Double>> reviewVectorsMapping = getReviewVectorsMapping(spark);

        JavaPairRDD<String, List<Double>> reviewMonthlyVectorsMapping = reviewVectorsMapping.reduceByKey((i1, i2) -> listPlus(i1, i2));

        JavaRDD<LabeledPoint> reviewMonthlyLP = reviewMonthlyVectorsMapping.map(s -> {
            String key = s._1();
            if(trendMonthlyMap.containsKey(key)) {
                double label = normalize(trendMonthlyMap.get(key));
                List<Double> list = s._2();
                double[] darray = new double[list.size()];
                for(int i = 0; i < list.size(); ++i) {
                    darray[i] = list.get(i);
                }
                return new LabeledPoint(label, Vectors.dense(darray));
            }
            else
                return null;
        })
                .filter(Objects::nonNull);

        reviewMonthlyLP.saveAsTextFile(foutpath);
        return;
    }

    public Dataset<Row> generateLabeledPointForReviewWindowly(SparkSession spark) {

        JavaRDD<String> reviewlines = getReviewlines(spark);
        JavaRDD<String> trendlines = getTrendlines(spark);

        JavaRDD<Row> reviewjavardd = reviewlines
                .map(s -> {
                    return RowFactory.create(s.split("\t")[0], s.split("\t")[1]);
                });
        StructType schema = new StructType(new StructField[]{
                new StructField("key", DataTypes.StringType, false, Metadata.empty()),
                new StructField("text", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> reviewdf = spark.createDataFrame(reviewjavardd.rdd(), schema);
//        System.out.println(String.format("reviewdf = %d", reviewdf.count()));

        JavaPairRDD<String, String> trendMonthToWindow = getTrendMonthToWindow(trendlines);
        JavaRDD<Row> trendMonthToWindowjavardd = trendMonthToWindow
                .map(s -> RowFactory.create(s._1(), s._2()));
        StructType schema2 = new StructType(new StructField[]{
                new StructField("key", DataTypes.StringType, false, Metadata.empty()),
                new StructField("window", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> trendMonthToWindowdf = spark.createDataFrame(trendMonthToWindowjavardd.rdd(), schema2);
//        trendMonthToWindowdf.write().json(String.format("%strendMonthToWindowdf", prepath));

        Dataset<Row> joindf = reviewdf
                .join(trendMonthToWindowdf, "key")
                .select("window", "text");
//        System.out.println(String.format("joindf = %d", joindf.count()));
//        joindf.show(false);
//        joindf.write().json(String.format("%sjoindf", prepath));

        JavaRDD<Row> aggjavardd = joindf
                .toJavaRDD()
                .mapToPair(s -> new Tuple2<String, String>(s.getAs("window"), s.getAs("text")))
                .reduceByKey((s1, s2) -> String.format("%s %s", s1, s2))
                .map(s -> RowFactory.create(s._1(), s._2().trim().split(" ")));
//        System.out.println(String.format("aggjavardd = %d", aggjavardd.count()));
        StructType schema4 = new StructType(new StructField[]{
                new StructField("window", DataTypes.StringType, false, Metadata.empty()),
                new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });
        Dataset<Row> aggdf = spark
                .createDataFrame(aggjavardd, schema4)
                .distinct();
//        aggdf.show(false);
//        aggdf.write().json(String.format("%saggdf", prepath));
//        System.out.println(String.format("aggdf = %d", aggdf.count()));

        JavaPairRDD<String, Double> trendWindowlyMapping = getTrendWindowLyMapping(trendlines)
                .mapToPair(s -> new Tuple2<>(s._1(), normalize(s._2())));
        JavaRDD<Row> trendWindowlyMappingjavardd = trendWindowlyMapping
                .map(s -> RowFactory.create(s._1(), s._2()));
        StructType schema3 = new StructType(new StructField[]{
                new StructField("window", DataTypes.StringType, false, Metadata.empty()),
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> trendWindowlyMappingdf = spark.createDataFrame(trendWindowlyMappingjavardd, schema3);
//        System.out.println(String.format("trendwindow = %d", trendWindowlyMappingdf.count()));

        trendWindowlyMappingdf = trendWindowlyMappingdf.distinct();
        System.out.println(String.format("trendwindow = %d", trendWindowlyMappingdf.count()));

        aggdf = aggdf
                .join(trendWindowlyMappingdf, "window")
                .select("label", "text", "window")
                .distinct();
        System.out.println(String.format("aggdf2 = %d", aggdf.count()));
//        aggdf.show(false);

        /*
        Map<String, String> trendMonthToWindowMap = trendMonthToWindow.collectAsMap();

        JavaPairRDD<String, String> pair = reviewlines
                .mapToPair(s -> {
                    String[] splitstring = s.split("\t");
                    String key = splitstring[0].trim();
                    if(trendMonthToWindowMap.containsKey(key)) {
                        String value = trendMonthToWindowMap.get(key);
                        return new Tuple2<>(value, splitstring[1].trim());
                    }
                    else
                        return null;
                })
                .filter(s -> {return s != null;})
                .reduceByKey((l1, l2) -> {
                    return String.format("%s %s", l1, l2);
                })
                ;

//        System.out.println(String.format("pair = %d", pair.count()));
        pair.saveAsTextFile(String.format("%spair", prepath));


        Map<String, Double> trendWindowlyMap = trendWindowlyMapping.collectAsMap();

        System.out.println(String.format("trendWindowlyMap = %d", trendWindowlyMap.size()));

        JavaRDD<Row> data = pair
                .map(s -> {
                    double label = normalize(trendWindowlyMap.get(s._1()));
                    return RowFactory.create(label, Arrays.asList(s._2().split(" |\t|\n|\\.")));
                });
//        System.out.println(String.format("data = %d", data.count()));

        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });
        Dataset<Row> df = spark.createDataFrame(
                data.collect(),
                schema);
//        df.show(false);

        */
        return aggdf;
    }

    public void bagOfWordModel(SparkSession spark) throws Exception {

//        /*
//        Dataset<Row> df = generateLabeledPointForReview(spark, word2VecModel, reviewlines, trendlines);
        Dataset<Row> df = generateLabeledPointForReviewWindowly(spark);

        int cvVocabSize = 10000;
        int chiNumTopFeatures = 1000;

//        /*
        CountVectorizerModel countVectorizerModel = new CountVectorizer()
                .setInputCol("text")
                .setOutputCol("feature")
                .setMinDF(1)
//                .setMinDF(10000)
//                .setVocabSize(10)
                .setVocabSize(cvVocabSize)
                .fit(df);
        Dataset<Row> cvdf = countVectorizerModel
                .transform(df)
                .drop("text");
//        cvdf.show(false);

//        cvdf.foreach(s -> {
//            System.out.println(new LabeledPoint(s.getAs("label"), Vectors.fromML(s.getAs("feature"))).toString());
//        });

        ChiSqSelectorModel chiSqSelectorModel = new ChiSqSelector()
                .setFeaturesCol("feature")
                .setLabelCol("label")
                .setOutputCol("chi")
//                .setNumTopFeatures(8)
                .setNumTopFeatures(chiNumTopFeatures)
                .fit(cvdf);
        Dataset<Row> chidf = chiSqSelectorModel
                .transform(cvdf)
                .drop("feature");
//        chidf.show(false);
//        chidf.write().json(String.format("%sCHIDF", prepath));

//        */
//        Dataset<Row> chidf = spark.read().json(String.format("%sCHIDF", prepath));
//        chidf.show(false);
//        /*
        Dataset<Row>[] chidfsplit = chidf.randomSplit(new double[]{0.8, 0.2});

        LogisticRegressionModel logisticRegressionModel = new LogisticRegression()
                .setFeaturesCol("chi")
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setProbabilityCol("prob")
                .fit(chidfsplit[0]);

//        logisticRegressionModel.save(String.format("%sLRModel", prepath));

        Dataset<Row> lrdf1 = logisticRegressionModel
                .transform(chidfsplit[0])
                .drop("chi")
                .drop("rawPrediction")
                .drop("prob");
//        lrdf1
////                .select("label", "prediction")
//                .limit(20)
//                .show(false);
//        lrdf1.write().json(prepath_dumbo+"ClusterTestOut");
        JavaRDD<Tuple2<Object, Object>> lrjavardd1 = lrdf1.javaRDD().map(s -> {
            double t1 = (double)s.getAs("prediction") > 10 ? 1 : 0;
            double t2 = (double)s.getAs("label") > 10 ? 1 : 0;
            return new Tuple2<>(t1, t2);
        });
        BinaryClassificationMetrics binaryClassificationMetrics1 = new BinaryClassificationMetrics(lrjavardd1.rdd());
        List<Row> outlist = new ArrayList<>();
        outlist.add(RowFactory.create(String.format("lrdf1 = %d", lrdf1.count())));
        outlist.add(RowFactory.create(String.format("precisionByThresHold1 size = %d:\n%s", binaryClassificationMetrics1.precisionByThreshold().count(), binaryClassificationMetrics1.precisionByThreshold().toJavaRDD().collect().toString())));
        outlist.add(RowFactory.create(String.format("recallByThresHold1 size = %d:\n%s", binaryClassificationMetrics1.recallByThreshold().count(), binaryClassificationMetrics1.recallByThreshold().toJavaRDD().collect().toString())));
        outlist.add(RowFactory.create(String.format("fMeasureByThresHold1 size = %d:\n%s", binaryClassificationMetrics1.fMeasureByThreshold().count(), binaryClassificationMetrics1.fMeasureByThreshold().toJavaRDD().collect().toString())));
        outlist.add(RowFactory.create(String.format("areaUnderPR1 = %f", binaryClassificationMetrics1.areaUnderPR())));
        outlist.add(RowFactory.create(String.format("areaUnderROC1 = %f", binaryClassificationMetrics1.areaUnderROC())));

        Dataset<Row> lrdf2 = logisticRegressionModel
                .transform(chidfsplit[1])
                .drop("chi")
                .drop("rawPrediction")
                .drop("prob");
//        lrdf2
////                .select("label", "prediction")
//                .limit(20)
//                .show(false);
        JavaRDD<Tuple2<Object, Object>> lrjavardd2 = lrdf2.javaRDD().map(s -> {
            double t1 = (double)s.getAs("prediction") > 10 ? 1 : 0;
            double t2 = (double)s.getAs("label") > 10 ? 1 : 0;
            return new Tuple2<>(t1, t2);
        });
        BinaryClassificationMetrics binaryClassificationMetrics2 = new BinaryClassificationMetrics(lrjavardd2.rdd());
        outlist.add(RowFactory.create(String.format("lrdf2 = %d", lrdf2.count())));
        outlist.add(RowFactory.create(String.format("precisionByThresHold2 size = %d:\n%s", binaryClassificationMetrics2.precisionByThreshold().count(), binaryClassificationMetrics2.precisionByThreshold().toJavaRDD().collect().toString())));
        outlist.add(RowFactory.create(String.format("recallByThresHold2 size = %d:\n%s", binaryClassificationMetrics2.recallByThreshold().count(), binaryClassificationMetrics2.recallByThreshold().toJavaRDD().collect().toString())));
        outlist.add(RowFactory.create(String.format("fMeasureByThresHold2 size = %d:\n%s", binaryClassificationMetrics2.fMeasureByThreshold().count(), binaryClassificationMetrics2.fMeasureByThreshold().toJavaRDD().collect().toString())));
        outlist.add(RowFactory.create(String.format("areaUnderPR2 = %f", binaryClassificationMetrics2.areaUnderPR())));
        outlist.add(RowFactory.create(String.format("areaUnderROC2 = %f", binaryClassificationMetrics2.areaUnderROC())));

        StructType outschema = new StructType(new StructField[]{
                new StructField("content", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> outdf = spark.createDataFrame(
                outlist,
                outschema);
        outdf.show(false);
//        outdf.write().json(String.format("%sBOWout2", prepath));


//        Dataset<Row> lrdf = logisticRegressionModel.transform(chidfsplit[1]);
//        lrdf.show();

//        BinaryClassificationEvaluator binaryClassificationEvaluator = new BinaryClassificationEvaluator()
//                .setLabelCol("label")
//                .setRawPredictionCol("rawPrediction");

//        System.out.println(binaryClassificationEvaluator.explainParams());
//        System.out.println(binaryClassificationEvaluator.getMetricName());
//        System.out.println(binaryClassificationEvaluator.getParam("metricName").toString());

//        LogisticRegressionSummary logisticRegressionSummary = logisticRegressionModel.evaluate(chidfsplit[0]);
//        System.out.println(String.format("accuracy1 = %f", logisticRegressionSummary.accuracy()));
//        System.out.println(String.format("weightedPrecision1 = %f", logisticRegressionSummary.weightedPrecision()));
//        System.out.println(String.format("weightedRecall1 = %f", logisticRegressionSummary.weightedRecall()));
//        System.out.println(String.format("weightedFMeasure1 = %f", logisticRegressionSummary.weightedFMeasure()));

//        */

        return;
    }

    public static JavaPairRDD<String, Double> getTrendMonthlyMapping(JavaRDD<String> trendlines) {
        JavaPairRDD<String, Double> trendMonthlyMapping = trendlines.flatMapToPair(s -> {
            String[] splitstring = s.split("\t");
            // schema:
            // 0->name, 1->address, 2->business_id, 3->zip, 4->neighborhood, 5->city, 6->state, 7->review_count, 8->#5-stars,
            // 9->#4-starts, 10->#3stars, 11->#2stars, 12->#1stars, 13->has_trend, 14->yelping_since, 15->#time_window
            // format of each time window: start, end, rating, trend
            int timeWindowCnt = Integer.parseInt(splitstring[15]);
            List<Tuple2<String, Double>> list = new ArrayList<Tuple2<String, Double>>();
            for(int i = 0; i < timeWindowCnt-1; ++i) {
                String startDate = splitstring[15+i*4+1];
                String endDate = splitstring[15+i*4+2];
                Double trend = Double.parseDouble(splitstring[15+i*4+4]);
                for(String str: calDateBetween(startDate, endDate)) {
                    String key = String.format("%s,%s", splitstring[2], str);
                    list.add(new Tuple2<>(key, trend));
                }
            }
            return list.iterator();
        });
        return trendMonthlyMapping;
    }

    public static JavaPairRDD<String, String> getTrendMonthToWindow(JavaRDD<String> trendlines) {
        JavaPairRDD<String, String> res = trendlines
                .flatMapToPair(s -> {
                    String[] splitstring = s.split("\t");
                    // schema:
                    // 0->name, 1->address, 2->business_id, 3->zip, 4->neighborhood, 5->city, 6->state, 7->review_count, 8->#5-stars,
                    // 9->#4-starts, 10->#3stars, 11->#2stars, 12->#1stars, 13->has_trend, 14->yelping_since, 15->#time_window
                    // format of each time window: start, end, rating, trend
                    int timeWindowCnt = Integer.parseInt(splitstring[15]);
                    List<Tuple2<String, String>> list = new ArrayList<>();
                    for(int i = 0; i < timeWindowCnt-1; ++i) {
                        String startDate = splitstring[15+i*4+1];
                        String endDate = splitstring[15+i*4+2];
                        String value = String.format("%s,%s,%s", splitstring[2], startDate, endDate);
                        for(String str: calDateBetween(startDate, endDate)) {
                            String key = String.format("%s,%s", splitstring[2], str);
                            list.add(new Tuple2<>(key, value));
                        }
                    }
                    return list.iterator();
                });
        return res;
    }

    public static JavaPairRDD<String, Double> getTrendWindowLyMapping(JavaRDD<String> trendlines) {
        JavaPairRDD<String, Double> res = trendlines
                .flatMapToPair(s -> {
                    String[] splitstring = s.split("\t");
                    // schema:
                    // 0->name, 1->address, 2->business_id, 3->zip, 4->neighborhood, 5->city, 6->state, 7->review_count, 8->#5-stars,
                    // 9->#4-starts, 10->#3stars, 11->#2stars, 12->#1stars, 13->has_trend, 14->yelping_since, 15->#time_window
                    // format of each time window: start, end, rating, trend
                    int timeWindowCnt = Integer.parseInt(splitstring[15]);
                    List<Tuple2<String, Double>> list = new ArrayList<>();
                    for(int i = 0; i < timeWindowCnt-1; ++i) {
                        String startDate = splitstring[15+i*4+1];
                        String endDate = splitstring[15+i*4+2];
                        String key = String.format("%s,%s,%s", splitstring[2], startDate, endDate);
                        Double trend = Double.parseDouble(splitstring[15+i*4+4]);
                        list.add(new Tuple2<>(key, trend));
                    }
                    return list.iterator();
                });
        return res;
    }

    public JavaPairRDD<String, List<Double>> getReviewVectorsMapping(SparkSession spark) {
        JavaRDD<String> reviewlines = getReviewlines(spark);
        Word2VecModel word2VecModel = getWord2VecModel(spark);

        JavaPairRDD<String, List<Double>> reviewVectorsMapping = reviewlines.mapToPair(s -> {
            String[] splitstring = s.split("\t");
            // schema: 0->review_id, 1->user_id, 2->business_id, 3->starts, 4->date, 5->text, 6->useful, 7->funny, 8->cool
            String cleanedDate = cleanDate(splitstring[4]);
            String key = String.format("%s,%s", splitstring[2], cleanedDate);
            String[] wordset = splitstring[5].trim().split(" |\t|\n|\\.");
            List<Double> wordvector = new ArrayList<Double>();
            for(int i = 0; i < 128; ++i) {
                wordvector.add(0.0);
            }
            for(String str: wordset) {
                double[] darray = word2VecModel.transform(str).toArray();
                for(int i = 0; i < darray.length; ++i) {
                    wordvector.set(i, wordvector.get(i)+darray[i]);
                }
            }
            return new Tuple2<>(key, wordvector);
        });
        return reviewVectorsMapping;
    }

    public static String cleanDate(String da) {
        String res = "";
        da = da.trim();
        if(da.length() != 10)
            return "";
        try {
            int year = Integer.parseInt(da.substring(0, 4));
            res += String.format("%04d_", year);
        } catch(NumberFormatException e) {
            return "";
        }
        try {
            int month = Integer.parseInt(da.substring(5, 7));
            res += String.format("%02d", month-1);
        } catch(NumberFormatException e) {
            return "";
        }
        return res;
    }

    public static List<Double> listPlus(List<Double> l1, List<Double> l2) {
        if(l1.size() != 128 || l2.size() != 128) {

        }

        List<Double> res = new ArrayList<Double>();
        for(int i = 0; i < 128; ++i) {
            res.add(0.0);
        }
        for(int i = 0; i < 128; ++i) {
            res.set(i, l1.get(i)+l2.get(i));
        }
        return res;
    }

    public static List<String> calDateBetween(String startDate, String endDate) {
        List<String> res = new ArrayList<String>();
        int startYear = Integer.parseInt(startDate.substring(0, 4));
        int endYear = Integer.parseInt((endDate.substring(0, 4)));
        int startMonth = Integer.parseInt(startDate.substring(5, startDate.length()));
        int endMonth = Integer.parseInt(endDate.substring(5, endDate.length()));
        for(int i = startYear; i <= endYear; ++i) {
            for(int j = (i == startYear ? startMonth : 0); j <= (i == endYear ? endMonth : 11); ++j) {
                res.add(String.format("%04d_%02d", i, j));
            }
        }
        return res;
    }

    public static double normalize(double d) {
        d = d + 5.0;
        return d / 0.5;
    }

    public void reviewPredict(String[] args) throws Exception {
//        String labeledfilepath = prepath + "LabeledPointForWord";
//        String labeledfilepath = prepath + "LabeledPointForReview";
        String labeledfilepath = prepath + "LabeledPointForReviewMonthly";
//        String labeledfilepath = prepath + "LabeledPointForReviewWindowly";

        String statspath = prepath + "reviewPredictOut";
        PrintStream pout = new PrintStream(new FileOutputStream(new File(statspath), false));

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("reviewPredict")
                .getOrCreate();

//        JavaRDD<String> lines = spark
//                .read()
//                .textFile(labeledfilepath)
////                .limit(5)
//                .javaRDD();
//
//        JavaRDD<LabeledPoint> data = lines.map(s -> LabeledPoint.parse(s));
//        JavaRDD<LabeledPoint> data = MLUtils
//                .loadLabeledPoints(spark.sparkContext(), labeledfilepath)
//                .toJavaRDD();
//
//        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.8, 0.2}, 11L);
//
//        JavaRDD<LabeledPoint> training = splits[0].cache();
//
//        JavaRDD<LabeledPoint> test = splits[1];
//
//        LogisticRegressionModel logisticRegressionModel = new LogisticRegressionWithLBFGS()
//                .setNumClasses(21)
//                .run(training.rdd());
//
//        logisticRegressionModel.clearThreshold();
//
//        JavaPairRDD<Object, Object> predictionAndLabels = test.mapToPair(s -> {
//            double predict = logisticRegressionModel.predict(s.features());
//            double label = s.label();
//            return new Tuple2<>(predict, label);
//        });
//
//        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());
//
//        pout.println(String.format("accuracy = %f", metrics.accuracy()));
//        pout.println(String.format("weightedPrecision = %f", metrics.weightedPrecision()));
//        pout.println(String.format("weightedRecall = %f", metrics.weightedRecall()));
//        pout.println(String.format("weightedFMeasure = %f", metrics.weightedFMeasure()));
//
//        spark.stop();
        return;
    }

    public void ldaModel(SparkSession spark) throws Exception {


        Dataset<Row> df = generateLabeledPointForReviewWindowly(spark);

        CountVectorizerModel countVectorizerModel = new CountVectorizer()
                .setInputCol("text")
                .setOutputCol("feature")
                .setMinDF(1)
//                .setMinDF(10000)
                .setVocabSize(3000)
                .fit(df);
        Dataset<Row> cvdf = countVectorizerModel
                .transform(df)
                .drop("text");

//        countVectorizerModel.save(String.format("%sCVModel", prepath_local));


//        CountVectorizerModel countVectorizerModel = CountVectorizerModel.load(String.format("%sCVModel", prepath_local));

        String[] vocabulary = countVectorizerModel.vocabulary();
        for(String voc: vocabulary) {
            System.out.print(String.format("%s,", voc));
        }
        System.out.println();


        LDAModel ldaModel = new LDA()
                .setFeaturesCol("feature")
                .setK(7)
                .setMaxIter(30)
                .fit(cvdf);
//        ldaModel.write().save(String.format("%sLDAModel", prepath_local));


        double ll = ldaModel.logLikelihood(cvdf);
        double lp =ldaModel.logPerplexity(cvdf);
        System.out.println("The lower bound on the log likelihood of the entire corpus: " + ll);
        System.out.println("The upper bound on perplexity: " + lp);


//        LDAModel ldaModel = LocalLDAModel.load(String.format("%sLDAModel", prepath_local));

        // Describe topics.
        Dataset<Row> topics = ldaModel
                .describeTopics(30);
        System.out.println("The topics described by their top-weighted terms:");
        topics.show(false);

        JavaRDD<Row> topicsjavardd = topics
                .toJavaRDD()
                .map(s -> {
                    List<Integer> idxs = s.getList(1);
//                    int[] idxs = new int[idxlist.size()];
//                    for(int i = 0; i < idxlist.size(); ++i) {
//                        idxs[i] = idxlist.get(i);
//                    }
                    String[] words = new String[idxs.size()];
                    for(int i = 0; i < idxs.size(); ++i) {
                        words[i] = vocabulary[idxs.get(i)];
                    }
                    return RowFactory.create(s.getAs("topic"),
//                            idxs,
                            s.getAs("termIndices"),
                            words,
                            s.getAs("termWeights"));
                });

        StructType schema = new StructType(new StructField[]{
                new StructField("topic", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("termIndices", new ArrayType(DataTypes.IntegerType, true), false, Metadata.empty()),
                new StructField("terms", new ArrayType(DataTypes.StringType, true), false, Metadata.empty()),
                new StructField("termWeights", new ArrayType(DataTypes.DoubleType, true), false, Metadata.empty())
        });

        Dataset<Row> topicdf = spark.createDataFrame(topicsjavardd, schema);
        topicdf.show(false);



        // Shows the result.
        Dataset<Row> ldadf = ldaModel.transform(cvdf);
        ldadf.show(false);

        Dataset<Row>[] ldadfsplit = ldadf.randomSplit(new double[]{0.8, 0.2});

        LogisticRegressionModel logisticRegressionModel = new LogisticRegression()
                .setFeaturesCol("feature")
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setProbabilityCol("prob")
                .setMaxIter(100)
                .fit(ldadfsplit[0]);

        Dataset<Row> lrdf1 = logisticRegressionModel
                .transform(ldadfsplit[0])
                .drop("feature")
                .drop("rawPrediction")
                .drop("prob");
//        lrdf1
////                .select("label", "prediction")
//                .limit(20)
//                .show(false);
//        lrdf1.write().json(prepath_dumbo+"ClusterTestOut");
        JavaRDD<Tuple2<Object, Object>> lrjavardd1 = lrdf1.javaRDD().map(s -> {
            double t1 = (double)s.getAs("prediction") > 10 ? 1 : 0;
            double t2 = (double)s.getAs("label") > 10 ? 1 : 0;
            return new Tuple2<>(t1, t2);
        });
        BinaryClassificationMetrics binaryClassificationMetrics1 = new BinaryClassificationMetrics(lrjavardd1.rdd());
        List<Row> outlist = new ArrayList<>();
        outlist.add(RowFactory.create(String.format("lrdf1 = %d", lrdf1.count())));
        outlist.add(RowFactory.create(String.format("precisionByThresHold1 size = %d:\n%s", binaryClassificationMetrics1.precisionByThreshold().count(), binaryClassificationMetrics1.precisionByThreshold().toJavaRDD().collect().toString())));
        outlist.add(RowFactory.create(String.format("recallByThresHold1 size = %d:\n%s", binaryClassificationMetrics1.recallByThreshold().count(), binaryClassificationMetrics1.recallByThreshold().toJavaRDD().collect().toString())));
        outlist.add(RowFactory.create(String.format("fMeasureByThresHold1 size = %d:\n%s", binaryClassificationMetrics1.fMeasureByThreshold().count(), binaryClassificationMetrics1.fMeasureByThreshold().toJavaRDD().collect().toString())));
        outlist.add(RowFactory.create(String.format("areaUnderPR1 = %f", binaryClassificationMetrics1.areaUnderPR())));
        outlist.add(RowFactory.create(String.format("areaUnderROC1 = %f", binaryClassificationMetrics1.areaUnderROC())));

        Dataset<Row> lrdf2 = logisticRegressionModel
                .transform(ldadfsplit[1])
                .drop("feature")
                .drop("rawPrediction")
                .drop("prob");
//        lrdf2
////                .select("label", "prediction")
//                .limit(20)
//                .show(false);
        JavaRDD<Tuple2<Object, Object>> lrjavardd2 = lrdf2.javaRDD().map(s -> {
            double t1 = (double)s.getAs("prediction") > 10 ? 1 : 0;
            double t2 = (double)s.getAs("label") > 10 ? 1 : 0;
            return new Tuple2<>(t1, t2);
        });
        BinaryClassificationMetrics binaryClassificationMetrics2 = new BinaryClassificationMetrics(lrjavardd2.rdd());
        outlist.add(RowFactory.create(String.format("lrdf2 = %d", lrdf2.count())));
        outlist.add(RowFactory.create(String.format("precisionByThresHold2 size = %d:\n%s", binaryClassificationMetrics2.precisionByThreshold().count(), binaryClassificationMetrics2.precisionByThreshold().toJavaRDD().collect().toString())));
        outlist.add(RowFactory.create(String.format("recallByThresHold2 size = %d:\n%s", binaryClassificationMetrics2.recallByThreshold().count(), binaryClassificationMetrics2.recallByThreshold().toJavaRDD().collect().toString())));
        outlist.add(RowFactory.create(String.format("fMeasureByThresHold2 size = %d:\n%s", binaryClassificationMetrics2.fMeasureByThreshold().count(), binaryClassificationMetrics2.fMeasureByThreshold().toJavaRDD().collect().toString())));
        outlist.add(RowFactory.create(String.format("areaUnderPR2 = %f", binaryClassificationMetrics2.areaUnderPR())));
        outlist.add(RowFactory.create(String.format("areaUnderROC2 = %f", binaryClassificationMetrics2.areaUnderROC())));

        StructType outschema = new StructType(new StructField[]{
                new StructField("content", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> outdf = spark.createDataFrame(
                outlist,
                outschema);
        outdf.show(false);


        return;
    }

    public void reviewProcess(SparkSession spark) {
        JavaRDD<String> reviewlines = getReviewlines(spark);

        JavaRDD<String> res = reviewlines
                .map(s -> {
                    String[] splitstring = s.split("\t");
//                    if(splitstring.length < 9) {
//                        System.out.println(s);
//                        return null;
//                    }
//                     schema: 0->review_id, 1->user_id, 2->business_id, 3->starts, 4->date, 5->text, 6->useful, 7->funny, 8->cool
//                    System.out.println(s);
                    String cleanedDate = cleanDate(splitstring[4]);
                    String key = String.format("%s,%s", splitstring[2], cleanedDate);
                    String value = splitstring[5].trim();
                    if(value.length() == 0)
                        return null;
                    else
                        return String.format("%s\t%s", key, splitstring[5]);
//                    return s;
                })
                .filter(s -> {return s != null;});
        System.out.println(String.format("reviewfinal = %d", res.count()));
//        res.saveAsTextFile(String.format("%sreview_version1.2", prepath));
//        res.saveAsTextFile(String.format("%sreview_toronto_version3", prepath));
//        res.saveAsTextFile(String.format("%sreview_phoenix_version3", prepath));
        res.saveAsTextFile(String.format("%sreview_%s_version3", prepath, cityname));

//        res.saveAsTextFile(String.format("%strend_review_toronto_version1.2", prepath));
//        res.saveAsTextFile(String.format("%strend_review_phoenix_version1.2", prepath));




        return;
    }

    public JavaRDD<Row> getAslines(SparkSession spark) {
//        String aspath = prepath + "results";
//        String aspath = prepath + "results_phoenix";
//        String aspath = prepath + "results_toronto";
        String aspath = prepath + "results_" + cityname;

        int[] array = {0, 4,  5,  8, 11, 12, 18};
        Set<Integer> asneedsentiment = new HashSet<>();
        for(int i: array) {
            asneedsentiment.add(i);
        }

        JavaRDD<Row> aslines = spark
                .read()
                .textFile(aspath)
                .javaRDD()
                .map(s -> {
                    String[] splitstring = s.split(",");
                    double[] features = new double[22];
                    double rating = Double.parseDouble(splitstring[21]);
                    features[20] = rating;
//                    if(rating == 3)
//                        rating = 0;
                    if(rating == 1 || rating == 2)
                        rating = -1;
                    else
                        rating = 1;
                    for(int i = 0; i < 20; ++i) {
                        features[i] = Double.parseDouble(splitstring[i].trim());
                        if(asneedsentiment.contains(i))
                            features[i] = features[i]*rating;
                    }
                    features[21] = 1;
                    String cleanedDate = cleanDate(splitstring[22]);
                    String key = String.format("%s,%s", splitstring[20], cleanedDate);
                    return RowFactory.create(features, key);
                })
                .filter(s -> (s != null));
        return aslines;
    }

    public Dataset<Row> generateLabeledPointForASWindowly(SparkSession spark) throws Exception {
        JavaRDD<Row> aslines = getAslines(spark);
        JavaRDD<String> trendlines = getTrendlines(spark);

        StructType schema = new StructType(new StructField[]{
                new StructField("feature", new ArrayType(DataTypes.DoubleType, true), false, Metadata.empty()),
                new StructField("key", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> asdf = spark.createDataFrame(aslines.rdd(), schema);
//        System.out.println(String.format("asdf = %d", asdf.count()));
//        asdf.show(false);

        JavaPairRDD<String, String> trendMonthToWindow = getTrendMonthToWindow(trendlines);
        JavaRDD<Row> trendMonthToWindowjavardd = trendMonthToWindow
                .map(s -> RowFactory.create(s._1(), s._2()));
        StructType schema2 = new StructType(new StructField[]{
                new StructField("key", DataTypes.StringType, false, Metadata.empty()),
                new StructField("window", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> trendMonthToWindowdf = spark.createDataFrame(trendMonthToWindowjavardd.rdd(), schema2);
//        trendMonthToWindowdf.write().json(String.format("%strendMonthToWindowdf", prepath));

        Dataset<Row> joindf = asdf
                .join(trendMonthToWindowdf, "key")
                .select("window", "feature");
//        System.out.println(String.format("joindf = %d", joindf.count()));
//        joindf.show(false);
//        joindf.write().json(String.format("%sjoindf", prepath));

        JavaRDD<Row> aggjavardd = joindf
                .toJavaRDD()
                .mapToPair(s -> new Tuple2<String, List<Double>>(s.getAs("window"), s.getList(1)))
                .reduceByKey((s1, s2) -> {
                    List<Double> res = new ArrayList<>();
                    for(int i = 0; i < s2.size(); ++i) {
                        res.add(s1.get(i) + s2.get(i));
                    }
                    return res;
                })
                .map(s -> {
                    List<Double> fea = s._2();
                    double[] res = new double[21];
                    double cnt = fea.get(21);
                    for(int i = 0; i < 21; ++i) {
                        res[i] = fea.get(i) / cnt;
                    }
                    return RowFactory.create(s._1(), res);
                });
//        System.out.println(String.format("aggjavardd = %d", aggjavardd.count()));
        StructType schema4 = new StructType(new StructField[]{
                new StructField("window", DataTypes.StringType, false, Metadata.empty()),
                new StructField("feature", new ArrayType(DataTypes.DoubleType, true), false, Metadata.empty())
        });

        Dataset<Row> aggdf = spark.createDataFrame(aggjavardd, schema4);
//        aggdf.write().json(String.format("%saggdf", prepath));
//        System.out.println(String.format("aggdf = %d", aggdf.count()));
//        aggdf.show(false);

        JavaPairRDD<String, Double> trendWindowlyMapping = getTrendWindowLyMapping(trendlines)
                .mapToPair(s -> new Tuple2<>(s._1(), normalize(s._2())));
        JavaRDD<Row> trendWindowlyMappingjavardd = trendWindowlyMapping
                .map(s -> RowFactory.create(s._1(), s._2()));
        StructType schema3 = new StructType(new StructField[]{
                new StructField("window", DataTypes.StringType, false, Metadata.empty()),
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> trendWindowlyMappingdf = spark.createDataFrame(trendWindowlyMappingjavardd, schema3);

        aggdf = aggdf
                .join(trendWindowlyMappingdf, "window")
                .select("label", "feature", "window")
                .withColumnRenamed("feature", "as");
        System.out.println(String.format("aggdf2 = %d", aggdf.count()));
//        aggdf.show(false);

        return aggdf;
    }

    public void aspectSentimentModel(SparkSession spark) throws Exception {

        Dataset<Row> asdf = generateLabeledPointForASWindowly(spark);
        asdf.repartition(1).write().json(String.format("%sASDF3.0", prepath));

        /*
        Dataset<Row>[] asdfsplit = asdf.randomSplit(new double[]{0.8, 0.2});

        LogisticRegressionModel logisticRegressionModel = new LogisticRegression()
                .setFeaturesCol("feature")
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setProbabilityCol("prob")
                .setMaxIter(100)
                .fit(asdfsplit[0]);

        Dataset<Row> lrdf1 = logisticRegressionModel
                .transform(asdfsplit[0])
                .drop("feature")
                .drop("rawPrediction")
                .drop("prob");

        JavaRDD<Tuple2<Object, Object>> lrjavardd1 = lrdf1.javaRDD().map(s -> {
            double t1 = (double)s.getAs("prediction") > 10 ? 1 : 0;
            double t2 = (double)s.getAs("label") > 10 ? 1 : 0;
            return new Tuple2<>(t1, t2);
        });
        BinaryClassificationMetrics binaryClassificationMetrics1 = new BinaryClassificationMetrics(lrjavardd1.rdd());
        List<Row> outlist = new ArrayList<>();
        outlist.add(RowFactory.create(String.format("lrdf1 = %d", lrdf1.count())));
        outlist.add(RowFactory.create(String.format("precisionByThresHold1 size = %d:\n%s", binaryClassificationMetrics1.precisionByThreshold().count(), binaryClassificationMetrics1.precisionByThreshold().toJavaRDD().collect().toString())));
        outlist.add(RowFactory.create(String.format("recallByThresHold1 size = %d:\n%s", binaryClassificationMetrics1.recallByThreshold().count(), binaryClassificationMetrics1.recallByThreshold().toJavaRDD().collect().toString())));
        outlist.add(RowFactory.create(String.format("fMeasureByThresHold1 size = %d:\n%s", binaryClassificationMetrics1.fMeasureByThreshold().count(), binaryClassificationMetrics1.fMeasureByThreshold().toJavaRDD().collect().toString())));
        outlist.add(RowFactory.create(String.format("areaUnderPR1 = %f", binaryClassificationMetrics1.areaUnderPR())));
        outlist.add(RowFactory.create(String.format("areaUnderROC1 = %f", binaryClassificationMetrics1.areaUnderROC())));

        Dataset<Row> lrdf2 = logisticRegressionModel
                .transform(asdfsplit[1])
                .drop("feature")
                .drop("rawPrediction")
                .drop("prob");

        JavaRDD<Tuple2<Object, Object>> lrjavardd2 = lrdf2.javaRDD().map(s -> {
            double t1 = (double)s.getAs("prediction") > 10 ? 1 : 0;
            double t2 = (double)s.getAs("label") > 10 ? 1 : 0;
            return new Tuple2<>(t1, t2);
        });
        BinaryClassificationMetrics binaryClassificationMetrics2 = new BinaryClassificationMetrics(lrjavardd2.rdd());
        outlist.add(RowFactory.create(String.format("lrdf2 = %d", lrdf2.count())));
        outlist.add(RowFactory.create(String.format("precisionByThresHold2 size = %d:\n%s", binaryClassificationMetrics2.precisionByThreshold().count(), binaryClassificationMetrics2.precisionByThreshold().toJavaRDD().collect().toString())));
        outlist.add(RowFactory.create(String.format("recallByThresHold2 size = %d:\n%s", binaryClassificationMetrics2.recallByThreshold().count(), binaryClassificationMetrics2.recallByThreshold().toJavaRDD().collect().toString())));
        outlist.add(RowFactory.create(String.format("fMeasureByThresHold2 size = %d:\n%s", binaryClassificationMetrics2.fMeasureByThreshold().count(), binaryClassificationMetrics2.fMeasureByThreshold().toJavaRDD().collect().toString())));
        outlist.add(RowFactory.create(String.format("areaUnderPR2 = %f", binaryClassificationMetrics2.areaUnderPR())));
        outlist.add(RowFactory.create(String.format("areaUnderROC2 = %f", binaryClassificationMetrics2.areaUnderROC())));

        StructType outschema = new StructType(new StructField[]{
                new StructField("content", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> outdf = spark.createDataFrame(
                outlist,
                outschema);
        outdf.show(false);
        */
        return;
    }

    public void trendPreprocessing(SparkSession spark) throws Exception {
        String trendpath = prepath + "crawling_withtrend3.txt";

        JavaRDD<String> trendfinal = spark
                .read()
                .textFile(trendpath)
                .toJavaRDD()
                .map(s -> {
                    String city = s.split("\t")[5];
//                    System.out.println(city);
                    if(city.trim().equals("las vegas"))
                        return s;
                    else
                        return null;
                })
                .filter(s -> (s != null))
                .distinct()
                ;
        System.out.println(String.format("selected city = %d", trendfinal.count()));
        trendfinal.saveAsTextFile(String.format("%scrawling_withtrend3_lasvegas.txt", prepath));
        return;
    }

    public void GBTModel(SparkSession spark) throws Exception{
//        String gbttestpath = prepath + "gbttest.txt";
//        JavaRDD<Row> t1 = spark.
//                read()
//                .textFile(gbttestpath)
//                .toJavaRDD()
//                .map(s -> {
//                    String[] splitstring = s.split(",");
//                    double f0 = Double.parseDouble(splitstring[0].trim());
//                    double f1 = splitstring[1].trim().equals("\"male\"") ? 0 : 1;
//                    double[] f2 = new double[2];
//                    f2[0] = Double.parseDouble(splitstring[2].trim());
//                    f2[1] = Double.parseDouble(splitstring[3].trim());
//                    double f3 = splitstring[4].trim().equals("\"yes\"") ? 0 : 1;
//                    double[] f4 = new double[4];
//                    f4[0] = Double.parseDouble(splitstring[5].trim());
//                    f4[1] = Double.parseDouble(splitstring[6].trim());
//                    f4[2] = Double.parseDouble(splitstring[7].trim());
//                    f4[3] = Double.parseDouble(splitstring[8].trim());
//
//                    return RowFactory.create(f0, f1, f2, f3, f4);
//                });
//        StructType schema = new StructType(new StructField[]{
//                new StructField("f0", DataTypes.DoubleType, false, Metadata.empty()),
//                new StructField("f1", DataTypes.DoubleType, false, Metadata.empty()),
//                new StructField("f2", new ArrayType(DataTypes.DoubleType, true), false, Metadata.empty()),
//                new StructField("f3", DataTypes.DoubleType, false, Metadata.empty()),
//                new StructField("f4", new ArrayType(DataTypes.DoubleType, true), false, Metadata.empty())
//        });
//        Dataset<Row> df1 = spark.createDataFrame(t1.rdd(), schema);
//        df1.show(false);

        /*
        Dataset<Row> df = generateLabeledPointForReviewWindowly(spark);
        CountVectorizerModel countVectorizerModel = new CountVectorizer()
                .setInputCol("text")
                .setOutputCol("feature")
                .setMinDF(1)
//                .setMinDF(10000)
//                .setVocabSize(10)
                .setVocabSize(10000)
                .fit(df);
        String[] cvwords = countVectorizerModel.vocabulary();
        System.out.println();
        for(String s: cvwords) {
            System.out.print(String.format("%s,", s));
        }
        System.out.println();

        Dataset<Row> cvdf = countVectorizerModel
                .transform(df)
                .drop("text");
//        cvdf.show(false);
        ChiSqSelectorModel chiSqSelectorModel = new ChiSqSelector()
                .setFeaturesCol("feature")
                .setLabelCol("label")
                .setOutputCol("chi")
//                .setNumTopFeatures(8)
                .setNumTopFeatures(2000)
                .fit(cvdf)
                ;
        System.out.println();
        for(int i: chiSqSelectorModel.selectedFeatures()) {
            System.out.print(String.format("%d,", i));
        }
        System.out.println();

        System.out.println();
        for(int i: chiSqSelectorModel.selectedFeatures()) {
            System.out.print(String.format("%s,", cvwords[i]));
        }
        System.out.println();

        Dataset<Row> chidf = chiSqSelectorModel
                .transform(cvdf)
                .drop("feature")
                .drop("label")
                .distinct()
                .withColumnRenamed("window", "window_chi")
                ;
//        chidf.show(false);
//        System.out.println(String.format("chidf = %d", chidf.count()));
//        chidf = chidf.distinct();
        JavaRDD<Row> chidfjavardd = chidf
                .toJavaRDD()
                .map(s -> {
                    double[] chi = Vectors.fromML(s.getAs("chi")).toArray();
                    return RowFactory.create(s.getAs("window_chi"), chi);
                })
                ;
        StructType schema = new StructType(new StructField[]{
                new StructField("window_chi", DataTypes.StringType, false, Metadata.empty()),
                new StructField("chi", new ArrayType(DataTypes.DoubleType, true), false, Metadata.empty()),
        });
        chidf = spark.createDataFrame(chidfjavardd.rdd(), schema);
        System.out.println(String.format("chidf = %d", chidf.count()));
//        chidf.repartition(10).write().json(String.format("%sCHIDF_%s", prepath, cityname));
        */

//        /*
        Dataset<Row> chidf = spark
                .read()
//                .json(String.format("%sCHIDF_toronto", prepath));
                .json(String.format("%sCHIDF_%s", prepath, cityname));
        System.out.println(String.format("chidf = %d", chidf.count()));


//        */
//        chidf.limit(10).show(false);
//        chidf.printSchema();
//        JavaRDD<Row> chidfjavardd = chidf
//                .toJavaRDD()
//                .map(s -> {
//                    List<Double> chilist = s.getList(0);
//                    double[] chi = new double[8];
//                    for(int i = 0; i < 8; ++i) {
//                        chi[i] = chilist.get(i);
//                    }
//                    return RowFactory.create(s.getAs("window_chi"), chi);
//                })
//                ;
//        StructType schema = new StructType(new StructField[]{
//                new StructField("window_chi", DataTypes.StringType, false, Metadata.empty()),
//                new StructField("chi", new ArrayType(DataTypes.DoubleType, true), false, Metadata.empty()),
//        });
//        Dataset<Row> testdf = spark.createDataFrame(chidfjavardd.rdd(), schema);
//        testdf.limit(10).show(false);
//        System.out.println(String.format("testdf = %d", testdf.count()));
//        testdf.repartition(10).write().json(String.format("%sTESTDF", prepath));

//        /*
        Dataset<Row> asdf = generateLabeledPointForASWindowly(spark)
                .drop("label")
                .distinct()
                .withColumnRenamed("window", "window_as")
                ;
//        asdf.show(false);
//        System.out.println(String.format("asdf = %d", asdf.count()));
//        asdf = asdf.distinct();
        System.out.println(String.format("asdf = %d", asdf.count()));
//        asdf.repartition(1).write().json(String.format("%sASDF5.0", prepath));

//        chidf = chidf.join(asdf, chidf.col("window_chi").equalTo(asdf.col("window_as")))
//                .distinct();
//        System.out.println(chidf.count());
//        chidf.write().json(String.format("%sCHIDFCHECK", prepath));
//        */

//        /*
        JavaRDD<Row> geojavardd = spark
                .read()
//                .textFile(String.format("%sgeo_feature.txt", prepath))
                .textFile(String.format("%sgeo_feature_%s", prepath, cityname))
//                .textFile(String.format("%sgeo_feature_toronto", prepath))
                .toJavaRDD()
                .map(s -> {
//                    System.out.println(s);
                    String window = s.split("\t")[0].trim();
                    if(s.split("\t").length != 2)
                        return null;
                    String[] splitstring = s.split("\t")[1].trim().split(",");
                    double[] geo = new double[3];
                    for(int i = 0; i < 3; ++i) {
                        geo[i] = Double.parseDouble(splitstring[i]);
                    }
                    return RowFactory.create(window, geo);
                })
                .filter(s -> (s != null));
        StructType schema = new StructType(new StructField[]{
                new StructField("window_geo", DataTypes.StringType, false, Metadata.empty()),
                new StructField("geo", new ArrayType(DataTypes.DoubleType, true), false, Metadata.empty()),
        });
        Dataset<Row> geodf = spark
                .createDataFrame(geojavardd.rdd(), schema)
                .distinct()
                ;
//        geodf.show(false);
//        System.out.println(String.format("geo = %d", geodf.count()));
//        geodf = geodf.distinct();
        System.out.println(String.format("geo = %d", geodf.count()));

        JavaRDD<Row> mobjavardd = spark
                .read()
//                .textFile(String.format("%smobile_feature.txt", prepath))
                .textFile(String.format("%smobile_feature_%s", prepath, cityname))
//                .textFile(String.format("%smobile_feature_toronto", prepath))
                .toJavaRDD()
                .map(s -> {
                    if(s.split("\t").length != 2)
                        return null;
                    String window = s.split("\t")[0].trim();
                    String[] splitstring = s.split("\t")[1].trim().split(",");
                    double[] mob = new double[2];
                    for(int i = 0; i < 2; ++i) {
                        mob[i] = Double.parseDouble(splitstring[i]);
                    }
                    return RowFactory.create(window, mob);
                })
                .filter(s -> (s != null));
        StructType schema2 = new StructType(new StructField[]{
                new StructField("window_mob", DataTypes.StringType, false, Metadata.empty()),
                new StructField("mob", new ArrayType(DataTypes.DoubleType, true), false, Metadata.empty()),
        });
        Dataset<Row> mobdf = spark.createDataFrame(mobjavardd.rdd(), schema2)
                .distinct()
//                .limit(50)
                ;
//        mobdf.show(false);
//        System.out.println(String.format("mob = %d", mobdf.count()));
//        mobdf = mobdf.distinct();
        System.out.println(String.format("mob = %d", mobdf.count()));

        Dataset<Row> joindf = chidf
                .join(geodf, chidf.col("window_chi").equalTo(geodf.col("window_geo")), "left")
//                .select("window", "label", "chi", "mob")
                .drop("window_geo")
//                .distinct()
                ;
//        System.out.println(String.format("joindf = %d", joindf.count()));
//        joindf = joindf.distinct();
        System.out.println(String.format("joindf = %d", joindf.count()));

//        /*
        joindf = joindf
                .join(asdf, joindf.col("window_chi").equalTo(asdf.col("window_as")), "left")
//                .select("window", "label", "chi", "mob", "as")
                .drop("window_as")
//                .distinct()
                 ;
//        System.out.println(String.format("joindf = %d", joindf.count()));
//        joindf = joindf.distinct();
        System.out.println(String.format("joindf = %d", joindf.count()));

        joindf = joindf
                .join(mobdf, joindf.col("window_chi").equalTo(mobdf.col("window_mob")), "left")
//                .select("window", "label", "geo", "mob", "as", "chi")
                .drop("window_mob")
//                .distinct()
                ;
//        joindf.show(false);
//        System.out.println(String.format("joindf = %d", joindf.count()));
//        joindf = joindf.distinct();
        System.out.println(String.format("joindf = %d", joindf.count()));
//        joindf.repartition(1).write().json(String.format("%sJOINDF_1w_2k_3.0", prepath));

//        joindf.limit(10).show(false);

//        */
//        /*
        JavaRDD<String> trendlines = getTrendlines(spark);
        JavaPairRDD<String, Double> trendWindowlyMapping = getTrendWindowLyMapping(trendlines)
                .mapToPair(s -> new Tuple2<>(s._1(), normalize(s._2())));
        JavaRDD<Row> trendWindowlyMappingjavardd = trendWindowlyMapping
                .map(s -> RowFactory.create(s._1(), s._2()));
        StructType schema_trend = new StructType(new StructField[]{
                new StructField("window", DataTypes.StringType, false, Metadata.empty()),
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> trendWindowlyMappingdf = spark
                .createDataFrame(trendWindowlyMappingjavardd, schema_trend)
                .distinct()
                ;
        System.out.println(String.format("trendwindow = %d", trendWindowlyMappingdf.count()));

        joindf = joindf
                .join(trendWindowlyMappingdf, joindf.col("window_chi").equalTo(trendWindowlyMappingdf.col("window")), "left")
                .drop("window")
                .distinct()
                ;
        System.out.println(String.format("joindf = %d", joindf.count()));
        joindf.limit(10).show(false);
        //window_chi, chi, geo, as, mob, label


        JavaRDD<Row> joinjavardd = joindf
                .toJavaRDD()
                .map(s -> {
//                    System.out.println(s.toString());
//                    Object o = s.get(2);
//                    System.out.println(o.toString());
                    double[] geo = new double[3];
                    if(s.isNullAt(2)) {
                        for(int i = 0; i < 3; ++i) {
                            geo[i] = 0.0;
                        }
                    }
                    else {
                        List<Double> geolist = s.getList(2);
                        for(int i = 0; i < 3; ++i) {
                            geo[i] = geolist.get(i);
                        }
                    }
                    double[] as = new double[20];
                    if(s.isNullAt(3)) {
                        for(int i = 0; i < 20; ++i) {
                            as[i] = 0.0;
                        }
                    }
                    else {
                        List<Double> aslist = s.getList(3);
                        for(int i = 0; i < 20; ++i) {
                            as[i] = aslist.get(i);
                        }
                    }
                    double[] mob = new double[2];
                    if(s.isNullAt(4)) {
                        for(int i = 0; i < 2; ++i) {
                            mob[i] = 0.0;
                        }
                    }
                    else {
                        List<Double> chilist = s.getList(4);
                        for(int i = 0; i < 2; ++i) {
                        mob[i] = chilist.get(i);
                        }
//                        chi = Vectors.fromML(s.getAs("chi")).toArray();
                    }
                    if(s.isNullAt(5)) {
                        System.out.println(s.toString());
                    }
                    return RowFactory.create(s.getAs("window_chi"), s.getAs("label"), mob, geo, as, s.getAs("chi"));
                })
                ;
        StructType schema3 = new StructType(new StructField[]{
                new StructField("window_chi", DataTypes.StringType, false, Metadata.empty()),
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("mob", new ArrayType(DataTypes.DoubleType, true), false, Metadata.empty()),
                new StructField("geo", new ArrayType(DataTypes.DoubleType, true), false, Metadata.empty()),
                new StructField("as", new ArrayType(DataTypes.DoubleType, true), false, Metadata.empty()),
                new StructField("chi", new ArrayType(DataTypes.DoubleType, true), false, Metadata.empty()),

        });
        Dataset<Row> finaldf = spark.createDataFrame(joinjavardd.rdd(), schema3)
                .distinct()
                .withColumnRenamed("window_chi", "window")
                ;
        System.out.println(String.format("finaldf = %d", finaldf.count()));
//        finaldf.limit(10).show(false);
        finaldf.repartition(1).write().json(String.format("%sFINALDF_%s_wss", prepath, cityname));
//        finaldf.repartition(1).write().json(String.format("%sFINALDF_toronto", prepath));

//        */

         /*
        Dataset<Row>[] dfsplit = finaldf.randomSplit(new double[]{0.8, 0.2});

        LogisticRegressionModel logisticRegressionModel = new LogisticRegression()
                .setFeaturesCol("chi")
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setProbabilityCol("prob")
                .setMaxIter(100)
                .fit(dfsplit[0]);

        Dataset<Row> lrdf1 = logisticRegressionModel
                .transform(dfsplit[0])
                .drop("chi")
                .drop("rawPrediction")
                .drop("prob");

        JavaRDD<Tuple2<Object, Object>> lrjavardd1 = lrdf1.javaRDD().map(s -> {
            double t1 = (double)s.getAs("prediction") > 10 ? 1 : 0;
            double t2 = (double)s.getAs("label") > 10 ? 1 : 0;
            return new Tuple2<>(t1, t2);
        });
        BinaryClassificationMetrics binaryClassificationMetrics1 = new BinaryClassificationMetrics(lrjavardd1.rdd());
        List<Row> outlist = new ArrayList<>();
        outlist.add(RowFactory.create(String.format("lrdf1 = %d", lrdf1.count())));
        outlist.add(RowFactory.create(String.format("precisionByThresHold1 size = %d:\n%s", binaryClassificationMetrics1.precisionByThreshold().count(), binaryClassificationMetrics1.precisionByThreshold().toJavaRDD().collect().toString())));
        outlist.add(RowFactory.create(String.format("recallByThresHold1 size = %d:\n%s", binaryClassificationMetrics1.recallByThreshold().count(), binaryClassificationMetrics1.recallByThreshold().toJavaRDD().collect().toString())));
        outlist.add(RowFactory.create(String.format("fMeasureByThresHold1 size = %d:\n%s", binaryClassificationMetrics1.fMeasureByThreshold().count(), binaryClassificationMetrics1.fMeasureByThreshold().toJavaRDD().collect().toString())));
        outlist.add(RowFactory.create(String.format("areaUnderPR1 = %f", binaryClassificationMetrics1.areaUnderPR())));
        outlist.add(RowFactory.create(String.format("areaUnderROC1 = %f", binaryClassificationMetrics1.areaUnderROC())));

        Dataset<Row> lrdf2 = logisticRegressionModel
                .transform(dfsplit[1])
                .drop("chi")
                .drop("rawPrediction")
                .drop("prob");

        JavaRDD<Tuple2<Object, Object>> lrjavardd2 = lrdf2.javaRDD().map(s -> {
            double t1 = (double)s.getAs("prediction") > 10 ? 1 : 0;
            double t2 = (double)s.getAs("label") > 10 ? 1 : 0;
            return new Tuple2<>(t1, t2);
        });
        BinaryClassificationMetrics binaryClassificationMetrics2 = new BinaryClassificationMetrics(lrjavardd2.rdd());
        outlist.add(RowFactory.create(String.format("lrdf2 = %d", lrdf2.count())));
        outlist.add(RowFactory.create(String.format("precisionByThresHold2 size = %d:\n%s", binaryClassificationMetrics2.precisionByThreshold().count(), binaryClassificationMetrics2.precisionByThreshold().toJavaRDD().collect().toString())));
        outlist.add(RowFactory.create(String.format("recallByThresHold2 size = %d:\n%s", binaryClassificationMetrics2.recallByThreshold().count(), binaryClassificationMetrics2.recallByThreshold().toJavaRDD().collect().toString())));
        outlist.add(RowFactory.create(String.format("fMeasureByThresHold2 size = %d:\n%s", binaryClassificationMetrics2.fMeasureByThreshold().count(), binaryClassificationMetrics2.fMeasureByThreshold().toJavaRDD().collect().toString())));
        outlist.add(RowFactory.create(String.format("areaUnderPR2 = %f", binaryClassificationMetrics2.areaUnderPR())));
        outlist.add(RowFactory.create(String.format("areaUnderROC2 = %f", binaryClassificationMetrics2.areaUnderROC())));

        StructType outschema = new StructType(new StructField[]{
                new StructField("content", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> outdf = spark.createDataFrame(
                outlist,
                outschema);
        outdf.show(false);
        */
        return;
    }

    public void cleanReview(SparkSession spark) {
        JavaRDD<String> reviewlines = getReviewlines(spark);
        reviewlines.repartition(200).saveAsTextFile(String.format("%scleanReviewPartitions_phoenix", prepath));
        return;
    }

    public void reviewFilter(SparkSession spark) {
//        String reviewpath1 = prepath + "review_toronto_version1.txt";
//        String reviewpath1 = prepath + "review_phoenix_version1.txt";
//        String reviewpath2 = prepath + "trend_review_toronto.txt";
//        String reviewpath2 = prepath + "trend_review_phoenix.txt";
        String reviewpath1 = prepath + "review_lasvegas_version1.txt";
        String reviewpath2 = prepath + "trend_review_lasvegas.txt";


        JavaRDD<Row> r1 = spark
                .read()
                .textFile(reviewpath1)
                .toJavaRDD()
                .map(s -> {
                    return RowFactory.create(s.split("\t")[0].trim(), s.trim());
                })
                ;
        StructType schema = new StructType(new StructField[]{
                new StructField("key", DataTypes.StringType, false, Metadata.empty()),
                new StructField("text", DataTypes.StringType, false, Metadata.empty()),

        });
        Dataset<Row> r1df = spark.createDataFrame(r1.rdd(), schema).distinct();

        JavaRDD<Row> r2 = spark
                .read()
                .textFile(reviewpath2)
                .toJavaRDD()
                .map(s -> {
                    return RowFactory.create(s.split("\t")[0].trim());
                })
                ;

        StructType schema2 = new StructType(new StructField[]{
                new StructField("key2", DataTypes.StringType, false, Metadata.empty()),
        });
        Dataset<Row> r2df = spark.createDataFrame(r2.rdd(), schema2).distinct();

        r1df = r2df.join(r1df, r2df.col("key2").equalTo(r1df.col("key")), "left")
                .distinct()
        .select("text");
        System.out.println(r1df.count());
        JavaRDD<String> r1rdd = r1df.toJavaRDD()
                .map(s -> s.getString(0));
//        r1rdd.saveAsTextFile(String.format("%sreview_toronto_version2", prepath));
//        r1rdd.saveAsTextFile(String.format("%sreview_phoenix_version2", prepath));
        r1rdd.saveAsTextFile(String.format("%sreview_lasvegas_version2", prepath));

    }

}

