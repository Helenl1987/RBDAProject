######################################################
#calculate: Density and Neighbor Entropy
######################################################

import math
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

df_all_lv = sqlContext.sql("select business_all.name, business_all.neighborhood, business_all.business_id, business_all.cate_normal, business_all.longitude,business_all.latitude,business_open.dates from zl2521.business_all,zl2521.business_open where business_all.city='las vegas' and business_all.business_id=business_open.business_id")

df_all_lv_d = df_all_lv.na.drop()

df_res_lv = sqlContext.sql("select business.business_id,business.latitude,business.longitude from zl2521.business where city='las vegas'")

df_res_lv_d = df_res_lv.na.drop()

res_list = sc.broadcast([[str(row['business_id']),str(row['latitude']),str(row['longitude'])] for row in df_res_lv_d.collect()])

all_list = sc.broadcast([[str(row['business_id']),str(row['name']),str(row['neighborhood']),str(row['cate_normal']),str(row['latitude']),str(row['longitude']),str(row['dates'])] for row in df_all_lv_d.collect()])


pt = sc.textFile("/user/zl2521/project/trend_play")

pt_lv = pt.filter(lambda x: x.split('\t')[5] == 'las vegas')

def add_lat_lon(line):
    fields = line.split("\t")
    fields = fields[:-2]
    id = fields[2]
    target = ["","",""]
    for row in res_list.value:
        if row[0]==id:
            target = row[:]
            break
    fields.insert(7,target[1])
    fields.insert(8,target[2])
    return '\t'.join(fields)


pt_add_lv = pt_lv.map(lambda x: add_lat_lon(x))
pt_add_lv_f = pt_add_lv.filter(lambda x: len(x.split('\t')[7]) > 0 and len(x.split('\t')[8]) > 0)

#sum of 18 categories
cates = sc.broadcast(["restaurants", "shopping", "home services",  "financial services", "professional services", "hotels & travel", "active life", "health & medical", "arts & entertainment", "pets", "public services & government", "event planning & services", "education", "religious organizations", "automotive", "food", "local services", "bars & pubs"])

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
    cate_normal = line['cate_normal']
    ot = line['dates']
    #count on neighborhood cate
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
        for i in range(len(cates.value)):
            cate = cates.value[i]
            count = 0
            for row in all_list.value:
                ds = row[6].split('-')
                ms = str(int(ds[1]) - 1).zfill(2)
                open = ds[0]+'_'+ms
                if open <= time and (row[2]=="" or row[2] == neigh) and row[3] == cate and (0 < distance(slat1,slon1,row[4],row[5]) < 500):
                    count += 1
            temp = time+'-'+str(i)+'-'+str(count)
            result.append(temp)
            all_count += count
        result.append(time+'-ALL-'+str(all_count))
        index -= 1
    result.insert(0, line['business_id'])
    result.insert(1, ots[0]+'_'+otms)
    return '\t'.join(result)


#pt_cate_count = pt_add_lv_f.map(lambda x: cate_count(x))

pt_cate_ts = pt_add_lv_f.map(lambda x: cate_count_ts(x))


def density_entropy(line):
    fields = line.split('\t')
    time_related = fields[2:]
    current = 0
    result = []
    while current < len(time_related):
        s = time_related[current+18]
        ts = s.split('-')[0]
        sum = int(s.split('-')[2])
        entropy = 0
        result.append(ts+'-D-'+str(sum))
        for i in range(18):
            c = time_related[current+i]
            cate_count = int(c.split('-')[2])
            x = float(cate_count)/float(sum)
            if x != 0:
                entropy += (-x)*math.log(x,2)
        result.append(ts+'-E-'+str(entropy))
        current += 19
    return '\t'.join(result)


pt_den_ent = pt_cate_ts.map(lambda x: density_entropy(x))













