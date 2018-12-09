import sys
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext

reload(sys)
sys.setdefaultencoding('utf-8')

bfile = open('/Users/zimoli/Downloads/RBDA-MCINTOSH/Project/RBDAProject/spark_data/checkin_lv.txt')

lines = []

for line in bfile:
	lines.append(line[:-1].split('\t'))

sc = SparkContext("local", "checkins")
sqlContext = HiveContext(sc)

rdd = sc.parallelize(lines)
df = sqlContext.createDataFrame(rdd, ['business_id','user_id','dates','main_cate'])
df_d = df.na.drop()

timestamps = sc.broadcast(["2018_10", "2018_09", "2018_08","2018_07", "2018_06", "2018_05", "2018_04", "2018_03","2018_02", "2018_01", "2018_00","2017_11", "2017_10", "2017_09", "2017_08", "2017_07", "2017_06", "2017_05", "2017_04", "2017_03", "2017_02", "2017_01", "2017_00","2016_11", "2016_10", "2016_09", "2016_08", "2016_07", "2016_06", "2016_05", "2016_04", "2016_03", "2016_02", "2016_01", "2016_00","2015_11", "2015_10", "2015_09", "2015_08", "2015_07", "2015_06", "2015_05", "2015_04", "2015_03", "2015_02", "2015_01", "2015_00", "2014_11", "2014_10", "2014_09", "2014_08", "2014_07", "2014_06", "2014_05", "2014_04", "2014_03", "2014_02", "2014_01", "2014_00"])
cates = sc.broadcast(["restaurants", "shopping", "home services",  "financial services", "professional services", "hotels & travel", "active life", "health & medical", "arts & entertainment", "pets", "public services & government", "event planning & services", "education", "religious organizations", "automotive",  "local services", "beauty & spas", "real estate", "nightlife", "mass media", "local flavor", "food"])

#change dates into timestamps range
def map_date(line):
	oridates = line['dates'].split('-')
	year = oridates[0]
	month = oridates[1]
	newmonth = str(int(month) - 1).zfill(2)
	return Row(business_id=line['business_id'], user_id=line['user_id'],dates=year+'_'+newmonth,main_cate=line['main_cate'])

mf = df_d.rdd
mf_f = mf.filter(lambda a: a['dates'].split('-')[0] >= "2014")
mf_n = mf_f.map(lambda line: map_date(line))

mf_df = sqlContext.createDataFrame(mf_n)
mf_df_count = mf_df.groupBy([mf_df.business_id,mf_df.dates]).count().withColumnRenamed('count','cate_count')
mf_df_all = mf_df.join(mf_df_count, (mf_df.business_id == mf_df_count.business_id) & (mf_df.dates == mf_df_count.dates)).drop('mf_df.business_id','mf_df.dates')
mf_rdd = mf_df_all.rdd.cache()

cates = sc.broadcast(["restaurants", "shopping", "home services",  "financial services", "professional services", "hotels & travel", "active life", "health & medical", "arts & entertainment", "pets", "public services & government", "event planning & services", "education", "religious organizations", "automotive",  "local services", "beauty & spas", "real estate", "nightlife", "mass media", "local flavor", "food"])

cates_dict = {}

def avg_cate(cates_dict):
	for cate in cates.value:
		mf_df_cate = mf_rdd.filter(lambda line: line['main_cate'].find(cate) != -1).map(lambda line: line['cate_count']).cache()
		number = mf_df_cate.count()
		ccount = mf_df_cate.reduce(lambda a,b: a+b)
		if number == 0:
			cates_dict[cate] = 0
		else:
			cates_dict[cate] = float(ccount) / float(number)

avg_cate(cates_dict)

cates_dict_b = sc.broadcast(cates_dict)

def peer_pop(line):
	bcates = line['main_cate'].split('|')
	n = 0
	avg = 0
	for bc in bcates:
		if bc in cates.value:
			avg += (cates_dict_b.value)[bc]
			n += 1
	if n != 0:
		avg = float(avg) / float(n)
	else:
		avg = 0
	if avg != 0:
		avg = float(line['cate_count']) / float(avg)
	return Row(business_id=line['business_id'], user_id=line['user_id'],\
			   dates=line['dates'],main_cate=line['main_cate'],\
			   cate_count=line['cate_count'],peer_pop=avg)

mf_all_feature = mf_rdd.map(lambda a: peer_pop(a))
mf_all_feature.saveAsPickleFile('/Users/zimoli/Downloads/RBDA-MCINTOSH/Project/RBDAProject/spark_data/checkins_lv_mobile')

