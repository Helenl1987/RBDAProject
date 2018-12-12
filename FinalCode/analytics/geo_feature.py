import math
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext
import sys
from pyspark.sql import Row
reload(sys)
sys.setdefaultencoding('utf-8')

sc = SparkContext("yarn", "Density")
sqlContext = HiveContext(sc)

#change city name on line 14,17,108 than can iterate
df_all_lv = sqlContext.sql("select business_all.name, business_all.neighborhood, business_all.business_id, business_all.main_cate, business_all.cate_count, business_all.longitude,business_all.latitude,business_all.cuisines,business_all.cuisine_count,business_open.dates from zl2521.business_all,zl2521.business_open where business_all.city='phoenix' and business_all.business_id=business_open.business_id")
df_all_lv_d = df_all_lv.na.drop()

df_res_lv = sqlContext.sql("select business_all.name, business_all.neighborhood, business_all.business_id, business_all.main_cate, business_all.cate_count, business_all.longitude,business_all.latitude,business_all.cuisines,business_all.cuisine_count,business_open.dates from zl2521.business_all,zl2521.business_open where business_all.city='phoenix' and business_all.business_id=business_open.business_id")

df_res_lv_d = df_res_lv.na.drop()

all_list = sc.broadcast([[str(row['business_id']),str(row['name']),str(row['neighborhood']),str(row['main_cate']),str(row['latitude']),str(row['longitude']),str(row['dates']),str(row['cuisines']),str(row['cate_count']),str(row['cuisine_count'])] for row in df_all_lv_d.collect()])

cates = sc.broadcast(["restaurants", "shopping", "home services",  "financial services", "professional services", "hotels & travel", "active life", "health & medical", "arts & entertainment", "pets", "public services & government", "event planning & services", "education", "religious organizations", "automotive",  "local services", "beauty & spas", "real estate", "nightlife", "mass media", "local flavor", "food"])

cuisines = sc.broadcast(["afghan","african","american (new)","american (traditional)","canadian (new)","canadian (traditional)","arabian","argentine","armenian","asian fusion","australian","austrian","bangladeshi","barbeque","basque","belgian","brasseries","brazilian","breakfast & brunch","british","buffets","burgers","burmese","cafes","cafeteria","cajun/creole","cambodian","caribbean","catalan","cheesesteaks","chicken shop","chicken wings","chinese","comfort food","creperies","cuban","czech","delis","diners","dinner theater","ethiopian","fast food","filipino","fish & chips","fondue","food court","food stands","french","game meat","gastropubs","german","gluten-free","greek","guamanian","halal","hawaiian","himalayan/nepalese","honduran","hong kong style cafe","hot dogs","hot pot","hungarian","iberian","indian","indonesian","irish","italian","japanese","kebab","korean","kosher","laotian","latin american","live/raw food","malaysian","mediterranean","mexican","middle eastern","modern european","mongolian","moroccan","new mexican cuisine","nicaraguan","noodles","pakistani","pan asia","persian/iranian","peruvian","pizza","polish","polynesian","pop-up restaurants","portuguese","poutineries","russian","salad","sandwiches","scandinavian","scottish","seafood","singaporean","slovakian","soul food","soup","southern","spanish","sri lankan","steakhouses","supper clubs","sushi bars","syrian","taiwanese","tapas bars","tapas/small plates","tex-mex","thai","turkish","ukrainian","uzbek","vegan","vegetarian","vietnamese","waffles","wraps"])

timestamps = sc.broadcast(["2018_10", "2018_09", "2018_08","2018_07", "2018_06", "2018_05", "2018_04", "2018_03","2018_02", "2018_01", "2018_00","2017_11", "2017_10", "2017_09", "2017_08", "2017_07", "2017_06", "2017_05", "2017_04", "2017_03", "2017_02", "2017_01", "2017_00","2016_11", "2016_10", "2016_09", "2016_08", "2016_07", "2016_06", "2016_05", "2016_04", "2016_03", "2016_02", "2016_01", "2016_00","2015_11", "2015_10", "2015_09", "2015_08", "2015_07", "2015_06", "2015_05", "2015_04", "2015_03", "2015_02", "2015_01", "2015_00", "2014_11", "2014_10", "2014_09", "2014_08", "2014_07", "2014_06", "2014_05", "2014_04", "2014_03", "2014_02", "2014_01", "2014_00"])

def distance(slat1, slon1, slat2, slon2):
    lat1 = float(slat1)
    lat2 = float(slat2)
    lon1 = float(slon1)
    lon2 = float(slon2)
    radius = 6371004
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) * math.sin(dlat / 2) +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) * math.sin(dlon / 2))
    return 2*math.atan2(math.sqrt(a), math.sqrt(1 - a))*radius

#consider the cate count in the neighborhood
def cate_count_ts(line):
    slat1 = line['latitude']
    slon1 = line['longitude']
    neigh = line['neighborhood']
    ot = line['dates']
    ccount = int(line['cuisine_count'])
    cuisines = line['cuisines'].split('|')
    cuisine_dict = {cu:0 for cu in cuisines}
    ots = ot.split('-')
    otms = str(int(ots[1]) - 1).zfill(2)
    ot_new = ots[0]+'_'+otms
    if ot_new < "2014_00":
        ot_new = "2014_00"
    index = timestamps.value.index(ot_new)
    result = []
    while index >= 0:
        time = timestamps.value[index]
        all_count = 0
        subr = {ca:0 for ca in cates.value}
        cuisine_dict = dict.fromkeys(cuisine_dict,0)
        for row in all_list.value:
            ds = row[6].split('-')
            ms = str(int(ds[1]) - 1).zfill(2)
            op = ds[0]+'_'+ms
            if op <= time and (row[2]=="" or row[2] == neigh) and (0 < distance(slat1,slon1,row[4],row[5]) < 500):
                main_categories = row[3].split('|')
                for mc in main_categories:
                    if mc in subr:
                        subr[mc] += 1
                        all_count += 1
                thiscuisines = row[7].split('|')
                if int(row[9]) != 0:
                    for tc in thiscuisines:
                        if tc in cuisine_dict:
                            cuisine_dict[tc] += 1
        ent = 0
        cates_dict = {}
        if all_count == 0:
            for cate,count in subr.items():
                cates_dict[cate] = count
            cates_dict['density'] = 0
            cates_dict['entropy'] = 0
            cates_dict['competitiveness'] = 0
        else:
            for cate,count in subr.items():
                cates_dict[cate] = count
                percent = float(count)/float(all_count)
                if percent != 0:
                    ent += (-percent)*math.log(percent,2)
            cates_dict['density'] = all_count
            cates_dict['entropy'] = ent
            competitive_count = 0
            for cui,count in cuisine_dict.items():
                competitive_count += count
            if ccount != 0:
                cates_dict['competitiveness'] = float(competitive_count) / (ccount*float(all_count)) # this should be all_count or res_count??
            else:
                cates_dict['competitiveness'] = 0
        result.append(Row(ts=time, business_id=line['business_id'], business_cate=line['main_cate'],dates=ots[0]+'_'+otms,features=Row(**cates_dict)))
        index -= 1
    return result


pt = df_res_lv_d.rdd
pt_cate_ts = pt.flatMap(lambda x: cate_count_ts(x))
pt_cate_ts.saveAsPickleFile("/user/zl2521/project/phoenix_cate_ts")