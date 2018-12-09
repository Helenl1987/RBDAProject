import math
import sys
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext
import pickle

reload(sys)
sys.setdefaultencoding('utf-8')

sc = SparkContext("local", "Density")
sqlContext = HiveContext(sc)

cates = sc.broadcast(["restaurants", "shopping", "home services",  "financial services", "professional services", "hotels & travel", "active life", "health & medical", "arts & entertainment", "pets", "public services & government", "event planning & services", "education", "religious organizations", "automotive",  "local services", "beauty & spas", "real estate", "nightlife", "mass media", "local flavor", "food"])

timestamps = sc.broadcast(["2018_10", "2018_09", "2018_08","2018_07", "2018_06", "2018_05", "2018_04", "2018_03","2018_02", "2018_01", "2018_00","2017_11", "2017_10", "2017_09", "2017_08", "2017_07", "2017_06", "2017_05", "2017_04", "2017_03", "2017_02", "2017_01", "2017_00","2016_11", "2016_10", "2016_09", "2016_08", "2016_07", "2016_06", "2016_05", "2016_04", "2016_03", "2016_02", "2016_01", "2016_00","2015_11", "2015_10", "2015_09", "2015_08", "2015_07", "2015_06", "2015_05", "2015_04", "2015_03", "2015_02", "2015_01", "2015_00", "2014_11", "2014_10", "2014_09", "2014_08", "2014_07", "2014_06", "2014_05", "2014_04", "2014_03", "2014_02", "2014_01", "2014_00"])

pt_cate_ts = sc.pickleFile('/Users/zimoli/Downloads/RBDA-MCINTOSH/Project/RBDAProject/spark_data/business_lv_process_5000')

###########read from pickle file############
count_dict = {}
cate_dict = {}
coeff_dict = {}
avg_dict = {}
#pt_den_ent = pt_cate_ts.map(lambda x: density_entropy(x))
def cate_count(cate_dict):
    for ts in timestamps.value:
        for cate in cates.value:
            cate_dict[ts+'_'+cate] = pt_cate_ts.filter(lambda x: x['ts']==ts and (x['main_cate'].find(cate) != -1))\
                                                .map(lambda line: line['features']['density'])\
                                                .reduce(lambda a,b: a+b)

def whole_count(count_dict):
    for ts in timestamps.value:
        count = 0
        for cate in cates.value:
            count += count_dict[ts+'_'+cate]
        count_dict[ts] = count

def att_coeff(coeff_dict):
    for ts in timestamps.value:
        for cate1 in cates.value:
            cate1_shops = pt_cate_ts.filter(lambda x: x['ts']==ts and (x['main_cate'].find(cate1) != -1)).cache()
            for cate2 in cates.value:
                temp_sum = cate1_shops.map(lambda a: float(a['features'][cate2])/float(a['features']['density']-a['features'][cate1]) if (a['features']['density']-a['features'][cate1]) > 0 else 0)\
                                      .reduce(lambda a,b: a+b)
                if cate_dict[ts+'_'+cate1] == 0 or cate_dict[ts+'_'+cate2] == 0:
                    coeff_dict[ts+'_'+cate1+'_'+cate2] = 0
                else:
                    coeff_dict[ts+'_'+cate1+'_'+cate2] = (float(count_dict[ts]-cate_dict[ts+'_'+cate1])/float(cate_dict[ts+'_'+cate1]*cate_dict[ts+'_'+cate2]))*temp_sum


def observed_avg(avg_dict):
    for ts in timestamps.value:
        for cate2 in cates.value:
            cate2_shops = pt_cate_ts.filter(lambda x: x['ts']==ts and (x['main_cate'].find(cate2) != -1)).cache()
            number = cate2_shops.count()
            if number == 0:
                for cate1 in cates.value:
                    avg_dict[ts+'_'+cate1+'_'+cate2] = 0
            else:
                for cate1 in cates.value:
                    temp_sum = cate2_shops.map(lambda a: a['features'][cate1])\
                                          .reduce(lambda a,b: a+b)
                    avg_dict[ts+'_'+cate1+'_'+cate2] = float(temp_sum) / float(number)



cate_count(cate_dict)
with open('/Users/zimoli/Downloads/RBDA-MCINTOSH/Project/RBDAProject/spark_data/cate_dict.pkl', 'wb') as f:
    pickle.dump(cate_dict, f, pickle.HIGHEST_PROTOCOL)

whole_count(count_dict)
with open('/Users/zimoli/Downloads/RBDA-MCINTOSH/Project/RBDAProject/spark_data/count_dict.pkl', 'wb') as f:
    pickle.dump(count_dict, f, pickle.HIGHEST_PROTOCOL)

att_coeff(coeff_dict)
with open('/Users/zimoli/Downloads/RBDA-MCINTOSH/Project/RBDAProject/spark_data/coeff_dict.pkl', 'wb') as f:
    pickle.dump(coeff_dict, f, pickle.HIGHEST_PROTOCOL)

observed_avg(avg_dict)
with open('/Users/zimoli/Downloads/RBDA-MCINTOSH/Project/RBDAProject/spark_data/avg_dict.pkl', 'wb') as f:
    pickle.dump(avg_dict, f, pickle.HIGHEST_PROTOCOL)

cate_dict_b = sc.broadcast(cate_dict)
count_dict_b = sc.broadcast(count_dict)
coeff_dict_b = sc.broadcast(coeff_dict)
avg_dict_b = sc.broadcast(avg_dict)

def jensen(line):
    buscates = line['main_cate'].split('|')
    buscates = [cate for cate in buscates if len(cate) > 0]
    cates_count = len(buscates)
    ts = line['ts']
    ssum = 0
    for cate in cates.value:
        for bc in buscates:
            key = ts+'_'+cate+'_'+bc
            coef = (coeff_dict_b.value)[key]
            avg = (avg_dict_b.value)[key]
            ssum += coef*(line['features'][cate]-avg)

    if cates_count == 0:
        return Row(business_id=line['business_id'], ts=line['ts'],\
                   main_cate=line['main_cate'], dates=line['dates'],\
                   density=line['features']['density'], entropy=line['features']['entropy'],\
                   comp=line['features']['competitiveness'], jensen=0)
    else:
        ssum = float(ssum) / float(cates_count)
        return Row(business_id=line['business_id'], ts=line['ts'],\
                   main_cate=line['main_cate'], dates=line['dates'],\
                   density=line['features']['density'], entropy=line['features']['entropy'],\
                   comp=line['features']['competitiveness'], jensen=ssum)

pt_all_features = pt_cate_ts.map(lambda line: jensen(line))
pt_all_features.saveAsPickeFile('/Users/zimoli/Downloads/RBDA-MCINTOSH/Project/RBDAProject/spark_data/geofeature_output')
