from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer, IndexToString
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.linalg import Vectors,DenseVector
from pyspark.sql import Row
from pyspark.sql import SparkSession

upath = "/user/zl2521/project/train_CITYNAME"

spark = SparkSession.builder.master("yarn").appName("RF_TEST_CITYNAME").config("spark.driver.maxResultSize", "10g").config("spark.executor.memory","4g").getOrCreate()

fdf = spark.read.json(upath)

def concat_rf_feats(line):
        all_feature = list(line["chi"]) + list(line["as"]) + list(line["geo"]) + list(line["mob"])
        return Row(feats=Vectors.dense(all_feature), label=line["label"])


fdf_rf_con = fdf.rdd.map(lambda x: concat_rf_feats(x)).toDF()
labelIndexer_rf = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(fdf_rf_con)

rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="feats", numTrees=100, maxDepth=6)

labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel", labels=labelIndexer_rf.labels)

pipeline = Pipeline(stages=[labelIndexer_rf, rf, labelConverter])
model = pipeline.fit(fdf_rf_con)


model_path = "/user/zl2521/project/rf_model_CITYNAME"
model.save(model_path)