from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer, IndexToString
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.linalg import Vectors,DenseVector
from pyspark.sql import Row
from pyspark.sql import SparkSession

def trans2sparse(line):
        indices = line["chi"]["indices"]
        values = line["chi"]["values"]
        vec = DenseVector(Vectors.sparse(2000, indices, values).toArray())
        return Row(chi=vec, window=line["window"])

upath = "/user/zl2521/project/train_CITYNAME"

spark = SparkSession.builder.master("yarn").appName("GBT_TEST_CITYNAME").config("spark.driver.maxResultSize", "10g").config("spark.executor.memory","4g").getOrCreate()

fdf = spark.read.json(upath)

def concat_feats(line):
        all_feature = list(line["chi"]) + list(line["as"]) + list(line["geo"]) + list(line["mob"])
        lab = 0
        if line["label"] > 10:
                lab = 1
        return Row(feats=Vectors.dense(all_feature), label=lab)


fdf_con = fdf.rdd.map(lambda x: concat_feats(x)).toDF()

labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(fdf_con)

gbt = GBTClassifier(labelCol="indexedLabel", featuresCol="feats", maxIter=200)
pipeline = Pipeline(stages=[labelIndexer, gbt])
model = pipeline.fit(fdf_con)

model_path = "/user/zl2521/project/gbt_model_CITYNAME"
model.save(model_path)