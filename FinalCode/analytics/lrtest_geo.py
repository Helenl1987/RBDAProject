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

bfile = open('/Users/zimoli/Desktop/crawling_withtrend2.txt')

target = []

id_dict = {}
for line in bfile:
	fields = line[:-1].split('\t')
	temp = []
	if fields[5] == 'toronto':
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

mobile_rdd = sc.pickleFile('/Users/zimoli/Downloads/RBDA-MCINTOSH/Project/RBDAProject/spark_data/checkins_toronto_mobile').cache()


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

def map_time_window(line):
	bid = str(line['business_id'])
	ts = str(line['dates'])
	key = bid+'_'+ts
	if key in trend_map_b.value:
		fields = ((trend_map_b.value)[key]).split('_')
		label = float(fields[-1])
		twindow = '_'.join(fields[:-1])
		return Row(business_id=line['business_id'],ts=line['dates'],business_cate=line['main_cate'],\
			   cate_count=float(line['cate_count']), peer_pop=float(line['peer_pop']),\
			   window=twindow,label=label)

mobile_rdd_win = mobile_rdd.map(lambda line: map_time_window(line))
mobile_rdd_f = mobile_rdd_win.filter(lambda line: line is not None)
mobile_df = mobile_rdd_f.toDF()

mobile_rdd_grouped = mobile_df.groupBy("business_id", "window").agg(sum("cate_count").alias('cc_sum'), sum("peer_pop").alias('pp_sum'), count("business_id").alias("data_count"), max("label").alias('label'))

mobile_rdd_avg0 = mobile_rdd_grouped.withColumn('cc_avg', mobile_rdd_grouped.cc_sum / mobile_rdd_grouped.data_count)
mobile_rdd_avg = mobile_rdd_avg0.withColumn('pp_avg', mobile_rdd_grouped.pp_sum / mobile_rdd_grouped.data_count)

def create_string_feature(line):
	sfeat = str(line['cc_avg'])+','+str(line['pp_avg'])
	ws = line['window'].split('_')
	new_window = ws[0]+'_'+ws[1]+','+ws[2]+'_'+ws[3]
	result = line['business_id']+','+new_window+'\t'+sfeat
	return result

mobile_data = mobile_rdd_avg.rdd

mobile_sf = mobile_data.map(lambda x: create_string_feature(x))

mobile_sf.saveAsTextFile("/Users/zimoli/Downloads/RBDA-MCINTOSH/Project/RBDAProject/mobile_feature_toronto")









