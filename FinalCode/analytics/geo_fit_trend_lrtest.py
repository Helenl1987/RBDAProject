import sys
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext
from pyspark.ml.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.sql.functions import *

reload(sys)
sys.setdefaultencoding('utf-8')

bfile = open('/Users/zimoli/Desktop/crawling_withtrend2.txt')

target = []

id_dict = {}
for line in bfile:
	fields = line[:-1].split('\t')
	temp = []
	if fields[5] == 'phoenix':
		if fields[2] not in id_dict:
			id_dict[fields[2]] = 1
			temp.append(fields[2])
			temp.extend(fields[14:])
			target.append('\t'.join(temp))

sc = SparkContext("local", "train")
sqlContext = HiveContext(sc)

rdd = sc.parallelize(target)

def flat_trend(line):
	fields = line.split('\t')
	bid = fields[0]
	num = int(fields[2])
	result = []
	for i in range(num-1):
		result.append([bid, fields[3+4*i], fields[3+4*i+1],fields[3+4*i+2],fields[3+4*i+3]])
	return result

rdd_flat = rdd.flatMap(lambda line: flat_trend(line))
trend_df = sqlContext.createDataFrame(rdd_flat, ['business_id', 'start', 'end', 'rating', 'trend'])

geo_rdd = sc.pickleFile('/Users/zimoli/Downloads/RBDA-MCINTOSH/Project/RBDAProject/phoenix_cate_ts').cache()


trend_list = sc.broadcast([[str(row['business_id']),str(row['start']),str(row['end']),str(row['rating']),str(row['trend'])] for row in trend_df.collect()])

trend_map = {}

def get_trend_map(trend_map):
	for trend in trend_list.value:
		if trend[1] != trend[2]:
			slabel = trend[4]
			label = "0.0"
			if float(slabel) > 0:
				label = "1.0"
			trend_map[trend[0]+'_'+trend[1]] = trend[1]+'_'+trend[2]+'_'+label
			trend_map[trend[0]+'_'+trend[2]] = trend[1]+'_'+trend[2]+'_'+label

get_trend_map(trend_map)

trend_map_b = sc.broadcast(trend_map)

def map_time_window(line):
	bid = str(line['business_id'])
	ts = str(line['ts'])
	key = bid+'_'+ts
	if key in trend_map_b.value:
		fields = ((trend_map_b.value)[key]).split('_')
		label = float(fields[-1])
		twindow = '_'.join(fields[:-1])
		return Row(business_id=line['business_id'],ts=line['ts'],business_cate=line['business_cate'],\
			   density=str(line['features']['density']), comp=str(line['features']['competitiveness']),\
			   entropy=str(line['features']['entropy']),window=twindow,label=label)

geo_rdd_win = geo_rdd.map(lambda line: map_time_window(line))
geo_rdd_f = geo_rdd_win.filter(lambda line: line is not None)
geo_df = geo_rdd_f.toDF()

geo_rdd_grouped = geo_df.groupBy("business_id", "window").agg(concat_ws('+', collect_list("density")).alias('dens'), concat_ws('+', collect_list("comp")).alias('comps'), concat_ws('+', collect_list("entropy")).alias('ents'), max("label").alias('label'))


def create_label_point(line):
	dens = line['dens'].split('+')
	comps = line['comps'].split('+')
	ents = line['ents'].split('+')
	if len(dens) == 2 and len(comps) == 2 and len(ents) == 2:
		if float(dens[1]) != 0:
			feat = [(float(dens[1])-float(dens[0]))/float(dens[1]), (float(ents[1])-float(ents[0]))*100, (float(comps[1])-float(comps[0]))*10000]
		else:
			feat = [0, (float(ents[1])-float(ents[0]))*100, (float(comps[1])-float(comps[0]))*10000]
		return LabeledPoint(float(line['label']), feat)

def create_string_feature(line):
	dens = line['dens'].split('+')
	comps = line['comps'].split('+')
	ents = line['ents'].split('+')
	ws = line['window'].split('_')
	new_window = ws[0]+'_'+ws[1]+','+ws[2]+'_'+ws[3]
	if len(dens) == 2 and len(comps) == 2 and len(ents) == 2:
		if float(dens[1]) != 0:
			feat = [(float(dens[1])-float(dens[0]))/float(dens[1]), (float(ents[1])-float(ents[0]))*100, (float(comps[1])-float(comps[0]))*10000]
		else:
			feat = [0, (float(ents[1])-float(ents[0]))*100, (float(comps[1])-float(comps[0]))*10000]
		feat = [str(f) for f in feat]
		sfeat = ','.join(feat)
		result = line['business_id']+','+new_window+'\t'+sfeat
		return result

geo_data = geo_rdd_grouped.rdd

geo_sf = geo_data.map(lambda x: create_string_feature(x))

#geo_sf.saveAsTextFile("/Users/zimoli/Downloads/RBDA-MCINTOSH/Project/RBDAProject/geo_feature_phoenix")




geo_set = geo_data.map(lambda line: create_label_point(line))
geo_set_f = geo_set.filter(lambda line: line is not None)


training, test = geo_set_f.randomSplit([0.8, 0.2], seed=11)
training.cache()


model = LogisticRegressionWithLBFGS.train(training,iterations=500, regParam=0.01, regType='l2')
labelsAndPreds = training.map(lambda p: (p.label, model.predict(p.features)))
trainErr = labelsAndPreds.filter(lambda lp: lp[0] != lp[1]).count() / float(training.count())
print("Training Error = " + str(trainErr))

predictionAndLabels = test.map(lambda lp: (float(model.predict(lp.features)), lp.label))
testErr = predictionAndLabels.filter(lambda lp: lp[0] != lp[1]).count() / float(test.count())
print("Testing Error = " + str(testErr))
# Instantiate metrics object
metrics = BinaryClassificationMetrics(predictionAndLabels)
# Area under precision-recall curve
print("Area under PR = %s" % metrics.areaUnderPR)
# Area under ROC curve
print("Area under ROC = %s" % metrics.areaUnderROC)














