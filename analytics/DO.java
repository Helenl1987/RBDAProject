//import org.apache.spark.SparkConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.*;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.ChiSqSelectorModel;
import org.apache.spark.ml.feature.ChiSqSelector;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
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
    static final String prepath_local = "/Users/apple/Downloads/18Fall/RBDA/RBDAProject/yelp_dataset/ProjectRecord/";
    static final String prepath_dumbo = "/user/jl10005/project/";
    static final String prepath_dumbo_local = "/home/jl10005/project/TryAnalytics/";

    static final String prepath = prepath_local;
//    static final String prepath = prepath_dumbo;

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

    public void preProcessing(String[] args) throws Exception {
//        String reviewpath = prepath + "review_version1.txt";
        String reviewpath = prepath + "review_version1.2";

        String trendpath = prepath + "crawling_withtrend.txt";

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
//                .config("spark.executor.memory", "4g")
                .master("local")
                .appName("wordToVecPreProcessing")
                .getOrCreate();

        JavaRDD<String> reviewlines = spark
                .read()
                .textFile(reviewpath)
//                .limit(500)
//                .limit(100000)
                .javaRDD();

        System.out.println(String.format("reviewlines = %d", reviewlines.count()));


        Word2VecModel word2VecModel = null;
//        word2VecModel = getWord2VecModel(spark, reviewlines);

        JavaRDD<String> trendlines = spark
                .read()
                .textFile(trendpath)
//                .limit(8)
                .javaRDD();

//        reviewProcess(spark, word2VecModel, reviewlines, trendlines);

        bagOfWordModel(spark, word2VecModel, reviewlines, trendlines);


        spark.stop();
        return;
    }

    public static Word2VecModel getWord2VecModel(SparkSession spark, JavaRDD<String> reviewlines) {
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

    public static void generateLabeledPointForWord(SparkSession spark, Word2VecModel word2VecModel, JavaRDD<String> reviewlines, JavaRDD<String> trendlines) {
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

    public static Dataset<Row> generateLabeledPointForReview(SparkSession spark, Word2VecModel word2VecModel, JavaRDD<String> reviewlines, JavaRDD<String> trendlines) {
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

    public static void generateLabeledPointForReviewMonthly(SparkSession spark, Word2VecModel word2VecModel, JavaRDD<String> reviewlines, JavaRDD<String> trendlines) {
        String foutpath = prepath + "LabeledPointForReviewMonthly";

        JavaPairRDD<String, Double> trendMonthlyMapping = getTrendMonthlyMapping(trendlines);

        Map<String, Double> trendMonthlyMap = trendMonthlyMapping.collectAsMap();

        JavaPairRDD<String, List<Double>> reviewVectorsMapping = getReviewVectorsMapping(word2VecModel, reviewlines);

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

    public static Dataset<Row> generateLabeledPointForReviewWindowly(SparkSession spark, Word2VecModel word2VecModel, JavaRDD<String> reviewlines, JavaRDD<String> trendlines) {

        JavaRDD<Row> reviewjavardd = reviewlines
                .map(s -> {
                    return RowFactory.create(s.split("\t")[0], s.split("\t")[1]);
                });
        StructType schema = new StructType(new StructField[]{
                new StructField("key", DataTypes.StringType, false, Metadata.empty()),
                new StructField("text", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> reviewdf = spark.createDataFrame(reviewjavardd.rdd(), schema);
        System.out.println(String.format("reviewdf = %d", reviewdf.count()));

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
        System.out.println(String.format("joindf = %d", joindf.count()));
//        joindf.show(false);
//        joindf.write().json(String.format("%sjoindf", prepath));

        JavaRDD<Row> aggjavardd = joindf
                .toJavaRDD()
                .mapToPair(s -> new Tuple2<String, String>(s.getAs("window"), s.getAs("text")))
                .reduceByKey((s1, s2) -> String.format("%s %s", s1, s2))
                .map(s -> RowFactory.create(s._1(), s._2().trim().split(" ")));
        System.out.println(String.format("aggjavardd = %d", aggjavardd.count()));
        StructType schema4 = new StructType(new StructField[]{
                new StructField("window", DataTypes.StringType, false, Metadata.empty()),
                new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });
        Dataset<Row> aggdf = spark.createDataFrame(aggjavardd, schema4);
//        aggdf.show(false);
//        aggdf.write().json(String.format("%saggdf", prepath));
        System.out.println(String.format("aggdf = %d", aggdf.count()));

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
                .select("label", "text");
        System.out.println(String.format("aggdf2 = %d", aggdf.count()));

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

    public static void bagOfWordModel(SparkSession spark, Word2VecModel word2VecModel, JavaRDD<String> reviewlines, JavaRDD<String> trendlines) throws Exception {

//        Dataset<Row> df = generateLabeledPointForReview(spark, word2VecModel, reviewlines, trendlines);
        Dataset<Row> df = generateLabeledPointForReviewWindowly(spark, word2VecModel, reviewlines, trendlines);

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
//        cvdf.show();

//        cvdf.foreach(s -> {
//            System.out.println(new LabeledPoint(s.getAs("label"), Vectors.fromML(s.getAs("feature"))).toString());
//        });

        ChiSqSelectorModel chiSqSelectorModel = new ChiSqSelector()
                .setFeaturesCol("feature")
                .setLabelCol("label")
                .setOutputCol("chi")
                .setNumTopFeatures(2000)
                .fit(cvdf);
        Dataset<Row> chidf = chiSqSelectorModel
                .transform(cvdf)
                .drop("feature");
//        chidf.show();

        Dataset<Row>[] chidfsplit = chidf.randomSplit(new double[]{0.8, 0.2});

        LogisticRegressionModel logisticRegressionModel = new LogisticRegression()
                .setFeaturesCol("chi")
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setProbabilityCol("prob")
                .fit(chidfsplit[0]);

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

    public static JavaPairRDD<String, List<Double>> getReviewVectorsMapping(Word2VecModel word2VecModel, JavaRDD<String> reviewlines) {
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

    public void ldaPreProcessing(String[] args) {

    }

    public void reviewProcess(SparkSession spark, Word2VecModel word2VecModel, JavaRDD<String> reviewlines, JavaRDD<String> trendlines) {
        JavaRDD<String> res = reviewlines
                .map(s -> {
                    String[] splitstring = s.split("\t");
                    // schema: 0->review_id, 1->user_id, 2->business_id, 3->starts, 4->date, 5->text, 6->useful, 7->funny, 8->cool
                    String cleanedDate = cleanDate(splitstring[4]);
                    String key = String.format("%s,%s", splitstring[2], cleanedDate);
                    String value = splitstring[5].trim();
                    if(value.length() == 0)
                        return null;
                    else
                        return String.format("%s\t%s", key, splitstring[5]);
                })
                .filter(s -> {return s != null;});
        res.saveAsTextFile(String.format("%sreview_version1.2", prepath));
        return;
    }
}

