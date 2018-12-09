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
from pyspark.sql.functions import sum, count, max

reload(sys)
sys.setdefaultencoding('utf-8')

bfile = open('/Users/zimoli/Downloads/RBDA-MCINTOSH/Project/RBDAProject/spark_data/crawling_withtrend.txt')

target = []

for line in bfile:
	fields = line[:-1].split('\t')
	temp = []
	if fields[5] == 'las vegas':
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

mobile_rdd = sc.pickleFile('/Users/zimoli/Downloads/RBDA-MCINTOSH/Project/RBDAProject/spark_data/checkins_lv_mobile').cache()


trend_list = sc.broadcast([[str(row['business_id']),str(row['start']),str(row['end']),str(row['rating']),str(row['trend'])] for row in trend_df.collect()])

trend_map = {}

def get_trend_map(trend_map):
	for trend in trend_list.value:
		temp = trend[1]
		slabel = trend[4]
		label = "0.0"
		if float(slabel) > 0:
			label = "1.0"
		while temp <= trend[2]:
			trend_map[trend[0]+'_'+temp] = trend[1]+'_'+trend[2]+'_'+label
			year = temp.split('_')[0]
			month = temp.split('_')[1]
			if int(month) == 11:
				temp = str(int(year)+1)+'_00'
			else:
				temp = year+'_'+str(int(month)+1).zfill(2)

get_trend_map(trend_map)

trend_map_b = sc.broadcast(trend_map)

traing_data = []

