from bs4 import BeautifulSoup
import urllib2
import time
import random
import json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def encode_search_http(name, loc):
    name_q =  urllib2.quote(name)
    loc_q = urllib2.quote(loc)
    http_addr = "https://www.yelp.com/search?find_desc=" + name_q + "&find_loc=" + loc_q
    return http_addr


def find_restaurant_page(page):
    searched_list1 = page.find_all("span", class_= "indexed-biz-name")
    searched_list2 = page.find_all("h3", class_="lemon--h3__373c0__5Q5tF heading--h3__373c0__1n4Of alternate__373c0__1uacp")
    #print "restaurants<span> on the search page: " + str(len(searched_list1))
    #print "restaurants<h3> on the search page: " + str(len(searched_list2))
    target_list = []
    if len(searched_list1) != 0:
        target_list = searched_list1
    if len(searched_list2) != 0:
        target_list = searched_list2
    if len(target_list) == 0:
        return None
    for res in target_list:
        #print res.get_text()
        if res.get_text().find("1.") != -1:
            temp = res.find('a')
            #print "length of <a> " + str(len(temp))
            target_res_http = "https://www.yelp.com" + temp.get('href')
            break
    return target_res_http

def get_rating_details(schema, target_res_http):
    html_res = urllib2.urlopen(target_res_http)
    page_res = BeautifulSoup(html_res, 'lxml')
    result = [-1 for i in range(64)]
    monthly_trend = page_res.find('div', id="rating-details-modal-content")
    if monthly_trend is not None:
        trend_data = monthly_trend.get("data-monthly-ratings")
        if len(trend_data) > 0:
            data_dict = json.loads(trend_data)
            for year in data_dict:
                for month,rate in data_dict[year]:
                    key = str(year)+'_'+str(month)
                    index = schema.index(key)
                    result[index] = rate
        else:
            print "uups, no rating trend found for this restaurant!!"
    else:
        print "uups, no rating trend found for this restaurant!!"
    overall_rating = page_res.find('table', class_="histogram histogram--alternating histogram--large")
    level_5 = overall_rating.find('tr', class_="histogram_row histogram_row--1")
    level_4 = overall_rating.find('tr', class_="histogram_row histogram_row--2")
    level_3 = overall_rating.find('tr', class_="histogram_row histogram_row--3")
    level_2 = overall_rating.find('tr', class_="histogram_row histogram_row--4")
    level_1 = overall_rating.find('tr', class_="histogram_row histogram_row--5")
    result[0] = int(level_5.get_text().split()[2])
    result[1] = int(level_4.get_text().split()[2])
    result[2] = int(level_3.get_text().split()[2])
    result[3] = int(level_2.get_text().split()[2])
    result[4] = int(level_1.get_text().split()[2])
    return result


if __name__ == "__main__":
    schema = ["star_5", "star_4", "star_3", "star_2", "star_1",
              "2018_10", "2018_9", "2018_8","2018_7", "2018_6", "2018_5", "2018_4", "2018_3","2018_2", "2018_1", "2018_0",
              "2017_11", "2017_10", "2017_9", "2017_8", "2017_7", "2017_6", "2017_5", "2017_4", "2017_3", "2017_2", "2017_1", "2017_0",
              "2016_11", "2016_10", "2016_9", "2016_8", "2016_7", "2016_6", "2016_5", "2016_4", "2016_3", "2016_2", "2016_1", "2016_0",
              "2015_11", "2015_10", "2015_9", "2015_8", "2015_7", "2015_6", "2015_5", "2015_4", "2015_3", "2015_2", "2015_1", "2015_0",
              "2014_11", "2014_10", "2014_9", "2014_8", "2014_7", "2014_6", "2014_5", "2014_4", "2014_3", "2014_2", "2014_1", "2014_0",]

    file = open('/Users/zimoli/Downloads/name_loc_for_crawl.tsv')
    for line in file:
        sstr = line.split('\t')
        name = sstr[0]
        loc = sstr[1]
        if len(name) == 0 or len(loc) == 0:
            print "for " + name + " @ " + loc + "not capable for crawl"
            continue
        http_search = encode_search_http(name, loc)
        print "search_page: " + http_search

        html = urllib2.urlopen(http_search)
        page = BeautifulSoup(html)
        target_res_http = find_restaurant_page(page)
        if target_res_http is None:
            print "for " + name + " @ " + loc + "no result found, or code error"
            continue
        print "restaurant page: " + target_res_http
        result = get_rating_details(schema, target_res_http)
        #change this following print into write can generate data following schema
        print result
        time.sleep(random.randint(1, 2) * .931467298)




