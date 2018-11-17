//import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
//import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;

import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.feature.ChiSqSelector;
import org.apache.spark.mllib.feature.ChiSqSelectorModel;
import org.apache.spark.mllib.feature.Word2Vec;
import org.apache.spark.mllib.feature.Word2VecModel;
//import org.apache.spark.sql.Dataset;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.sql.SparkSession;
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
        reviewAnalysis.reviewPredict(null);
    }
}



class ReviewAnalysis {
    public void preProcessing(String[] args) throws Exception {
//        String reviewpath = "/Users/apple/Downloads/18Fall/RBDA/RBDAProject/yelp_dataset/ProjectRecord/review_version1.txt";
//        String trendpath = "/Users/apple/Downloads/18Fall/RBDA/RBDAProject/yelp_dataset/ProjectRecord/crawling_withtrend.txt";

        String reviewpath = "/user/jl10005/project/review_version1.txt";
        String trendpath = "/user/jl10005/project/crawling_withtrend.txt";

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("wordToVecPreProcessing")
                .getOrCreate();

        JavaRDD<String> reviewlines = spark
                .read()
                .textFile(reviewpath)
//                .limit(50)
                .javaRDD();

        Word2VecModel word2VecModel = getWord2VecModel(spark, reviewlines);

        JavaRDD<String> trendlines = spark
                .read()
                .textFile(trendpath)
//                .limit(8)
                .javaRDD();

//        generateLabeledPointForWord(spark, word2VecModel, reviewlines, trendlines);
//        generateLabeledPointForReview(spark, word2VecModel, reviewlines, trendlines);
//        generateLabeledPointForReviewMonthly(spark, word2VecModel, reviewlines, trendlines);
//        generateLabeledPointForReviewWindowly(spark, word2VecModel, reviewlines, trendlines);

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
        String foutpath = "/user/jl10005/project/LabeledPointForWord";
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

    public static void generateLabeledPointForReview(SparkSession spark, Word2VecModel word2VecModel, JavaRDD<String> reviewlines, JavaRDD<String> trendlines) {
        String foutpath = "/user/jl10005/project/LabeledPointForReview";

        JavaPairRDD<String, Double> trendMonthlyMapping = getTrendMonthlyMapping(trendlines);

        Map<String, Double> trendMonthlyMap = trendMonthlyMapping.collectAsMap();

        JavaPairRDD<String, List<Double>> reviewVectorsMapping = getReviewVectorsMapping(word2VecModel, reviewlines);

        JavaRDD<LabeledPoint> reviewLP = reviewVectorsMapping.map(s -> {
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

        reviewLP.saveAsTextFile(foutpath);
        return;
    }

    public static void generateLabeledPointForReviewMonthly(SparkSession spark, Word2VecModel word2VecModel, JavaRDD<String> reviewlines, JavaRDD<String> trendlines) {
        String foutpath = "/user/jl10005/project/LabeledPointForReviewMonthly";

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

    public static void generateLabeledPointForReviewWindowly(SparkSession spark, Word2VecModel word2VecModel, JavaRDD<String> reviewlines, JavaRDD<String> trendlines) {

    }

    public static void bagOfWordModel(Word2VecModel word2VecModel) {

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
//        String labeledfilepath = "/user/jl10005/project/LabeledPointForWord";
//        String labeledfilepath = "/user/jl10005/project/LabeledPointForReview";
        String labeledfilepath = "/user/jl10005/project/LabeledPointForReviewMonthly";
//        String labeledfilepath = "/user/jl10005/project/LabeledPointForReviewWindowly";

        String statspath = "/user/jl10005/project/reviewPredictOut";
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
        JavaRDD<LabeledPoint> data = MLUtils
                .loadLabeledPoints(spark.sparkContext(), labeledfilepath)
                .toJavaRDD();

        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.8, 0.2}, 11L);

        JavaRDD<LabeledPoint> training = splits[0].cache();

        JavaRDD<LabeledPoint> test = splits[1];

        LogisticRegressionModel logisticRegressionModel = new LogisticRegressionWithLBFGS()
                .setNumClasses(21)
                .run(training.rdd());

        logisticRegressionModel.clearThreshold();

        JavaPairRDD<Object, Object> predictionAndLabels = test.mapToPair(s -> {
            double predict = logisticRegressionModel.predict(s.features());
            double label = s.label();
            return new Tuple2<>(predict, label);
        });

        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());

        pout.println(String.format("accuracy = %f", metrics.accuracy()));
        pout.println(String.format("weightedPrecision = %f", metrics.weightedPrecision()));
        pout.println(String.format("weightedRecall = %f", metrics.weightedRecall()));
        pout.println(String.format("weightedFMeasure = %f", metrics.weightedFMeasure()));

        spark.stop();
        return;
    }

    public void ldaPreProcessing(String[] args) {

    }

}

