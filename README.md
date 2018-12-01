# RBDAProject

## TODOList
* Yelp Rating Fail Case Try Again
* ZL:
    * business category
    * mobility feature
    * geography feature
    * restaurant inspection data
* JL:
    * trend spliting
        * done
    * review
        * BOW
            * weighted LR
            * review attribute (useful, funny, cool)
        * Embedding
        * Paragraph
        * LDA

## command - create table for business
```sql
create external table business (hours_Monday string, hours_Thursday string, hours_Friday string, hours_Wednesday string, hours_Tuesday string, hours_Saturday string, address string, city string, is_open int, latitude double, review_count int, stars double, name string, attributes_BikeParking string, attributes_BusinessAcceptsCreditCards string, attributes_RestaurantsDelivery string, attributes_BusinessParking_garage string, attributes_BusinessParking_street string, attributes_BusinessParking_validated string, attributes_BusinessParking_lot string, attributes_BusinessParking_valet string, attributes_NoiseLevel string, attributes_RestaurantsGoodForGroups string, attributes_RestaurantsPriceRange2 string, attributes_RestaurantsReservations string, attributes_RestaurantsTakeOut string, attributes_GoodForKids string, attributes_RestaurantsAttire string, attributes_HasTV string, attributes_OutdoorSeating string, neighborhood string, state string, categories string, postal_code string, business_id string, longitude double, hours_Sunday string, attributes_RestaurantsTableService string, attributes_Alcohol string, attributes_WheelchairAccessible string, attributes_Caters string, attributes_WiFi string, attributes_GoodForMeal_dessert string, attributes_GoodForMeal_latenight string, attributes_GoodForMeal_lunch string, attributes_GoodForMeal_dinner string, attributes_GoodForMeal_breakfast string, attributes_GoodForMeal_brunch string, attributes_Ambience_romantic string, attributes_Ambience_intimate string, attributes_Ambience_classy string, attributes_Ambience_hipster string, attributes_Ambience_touristy string, attributes_Ambience_trendy string, attributes_Ambience_upscale string, attributes_Ambience_casual string, hours string, attributes_Ambience_divey string) row format delimited fields terminated by '\t' location 'LOCATION'
```


## command - create table for user
```sql
create external table user (compliment_more int, compliment_writer int, compliment_funny int, average_stars double, cool int, review_count int, compliment_plain int, friends string, compliment_note int, fans int, elite string, compliment_profile int, user_id string, yelping_since string, compliment_hot int, name string, compliment_photos int, compliment_cool int, compliment_list int, compliment_cute int, useful int, funny int) row format delimited fields terminated by '\t' location 'LOCATION'
```


## command - create table for review
```sql
create external table review (review_id string, user_id string, business_id string, stars double, dates string, text string, useful int, funny int, cool int) row format delimited fields terminated by '\t' location '/user/REPLACE_BY_FILE_LOCATION';
```

## command - create table for checkin
```sql
create external table checkin (Mon_0 smallint, Mon_1 smallint, Mon_2 smallint, Mon_3 smallint, Mon_4 smallint, Mon_5 smallint, Mon_6 smallint, Mon_7 smallint, Mon_8 smallint, Mon_9 smallint, Mon_10 smallint, Mon_11 smallint, Mon_12 smallint, Mon_13 smallint, Mon_14 smallint, Mon_15 smallint, Mon_16 smallint, Mon_17 smallint, Mon_18 smallint, Mon_19 smallint, Mon_20 smallint, Mon_21 smallint, Mon_22 smallint, Mon_23 smallint, Tue_0 smallint, Tue_1 smallint, Tue_2 smallint, Tue_3 smallint, Tue_4 smallint, Tue_5 smallint, Tue_6 smallint, Tue_7 smallint, Tue_8 smallint, Tue_9 smallint, Tue_10 smallint, Tue_11 smallint, Tue_12 smallint, Tue_13 smallint, Tue_14 smallint, Tue_15 smallint, Tue_16 smallint, Tue_17 smallint, Tue_18 smallint, Tue_19 smallint, Tue_20 smallint, Tue_21 smallint, Tue_22 smallint, Tue_23 smallint, Wed_0 smallint, Wed_1 smallint, Wed_2 smallint, Wed_3 smallint, Wed_4 smallint, Wed_5 smallint, Wed_6 smallint, Wed_7 smallint, Wed_8 smallint, Wed_9 smallint, Wed_10 smallint, Wed_11 smallint, Wed_12 smallint, Wed_13 smallint, Wed_14 smallint, Wed_15 smallint, Wed_16 smallint, Wed_17 smallint, Wed_18 smallint, Wed_19 smallint, Wed_20 smallint, Wed_21 smallint, Wed_22 smallint, Wed_23 smallint, Thu_0 smallint, Thu_1 smallint, Thu_2 smallint, Thu_3 smallint, Thu_4 smallint, Thu_5 smallint, Thu_6 smallint, Thu_7 smallint, Thu_8 smallint, Thu_9 smallint, Thu_10 smallint, Thu_11 smallint, Thu_12 smallint, Thu_13 smallint, Thu_14 smallint, Thu_15 smallint, Thu_16 smallint, Thu_17 smallint, Thu_18 smallint, Thu_19 smallint, Thu_20 smallint, Thu_21 smallint, Thu_22 smallint, Thu_23 smallint, Fri_0 smallint, Fri_1 smallint, Fri_2 smallint, Fri_3 smallint, Fri_4 smallint, Fri_5 smallint, Fri_6 smallint, Fri_7 smallint, Fri_8 smallint, Fri_9 smallint, Fri_10 smallint, Fri_11 smallint, Fri_12 smallint, Fri_13 smallint, Fri_14 smallint, Fri_15 smallint, Fri_16 smallint, Fri_17 smallint, Fri_18 smallint, Fri_19 smallint, Fri_20 smallint, Fri_21 smallint, Fri_22 smallint, Fri_23 smallint, Sat_0 smallint, Sat_1 smallint, Sat_2 smallint, Sat_3 smallint, Sat_4 smallint, Sat_5 smallint, Sat_6 smallint, Sat_7 smallint, Sat_8 smallint, Sat_9 smallint, Sat_10 smallint, Sat_11 smallint, Sat_12 smallint, Sat_13 smallint, Sat_14 smallint, Sat_15 smallint, Sat_16 smallint, Sat_17 smallint, Sat_18 smallint, Sat_19 smallint, Sat_20 smallint, Sat_21 smallint, Sat_22 smallint, Sat_23 smallint, Sun_0 smallint, Sun_1 smallint, Sun_2 smallint, Sun_3 smallint, Sun_4 smallint, Sun_5 smallint, Sun_6 smallint, Sun_7 smallint, Sun_8 smallint, Sun_9 smallint, Sun_10 smallint, Sun_11 smallint, Sun_12 smallint, Sun_13 smallint, Sun_14 smallint, Sun_15 smallint, Sun_16 smallint, Sun_17 smallint, Sun_18 smallint, Sun_19 smallint, Sun_20 smallint, Sun_21 smallint, Sun_22 smallint, Sun_23 smallint, hour_0 smallint, hour_1 smallint, hour_2 smallint, hour_3 smallint, hour_4 smallint, hour_5 smallint, hour_6 smallint, hour_7 smallint, hour_8 smallint, hour_9 smallint, hour_10 smallint, hour_11 smallint, hour_12 smallint, hour_13 smallint, hour_14 smallint, hour_15 smallint, hour_16 smallint, hour_17 smallint, hour_18 smallint, hour_19 smallint, hour_20 smallint, hour_21 smallint, hour_22 smallint, hour_23 smallint, weekday int, weekend int, count_all int, business_id string) row format delimited fields terminated by '\t' location '/user/REPLACE_BY_FILE_LOCATION';
```

## command - create table for tip
```sql
create external table tip (text string, date string, likes int, business_id string, user_id string) row format delimited fields terminated by '\t' location '/user/REPLACE_BY_FILE_LOCATION';
```


## command - create table for yelp rating
```sql
create external table rating_details (name string, address string, business_id string,  zip string, neighborhood string, city string, state string, review_sum  int,  star_5  int,  star_4  int,  star_3  int,  star_2  int,  star_1  int,  has_trend  int, 2018_10  double,  2018_9  double,  2018_8  double, 2018_7  double,  2018_6  double,  2018_5  double,  2018_4  double,  2018_3  double, 2018_2  double,  2018_1  double,  2018_0  double, 2017_11  double,  2017_10  double,  2017_9  double,  2017_8  double,  2017_7  double,  2017_6  double,  2017_5  double,  2017_4  double,  2017_3  double,  2017_2  double,  2017_1  double,  2017_0  double, 2016_11  double,  2016_10  double,  2016_9  double,  2016_8  double,  2016_7  double,  2016_6  double,  2016_5  double,  2016_4  double,  2016_3  double,  2016_2  double,  2016_1  double,  2016_0  double, 2015_11  double,  2015_10  double,  2015_9  double,  2015_8  double,  2015_7  double,  2015_6  double,  2015_5  double,  2015_4  double,  2015_3  double,  2015_2  double,  2015_1  double,  2015_0  double, 2014_11  double,  2014_10  double,  2014_9  double,  2014_8  double,  2014_7  double,  2014_6  double,  2014_5  double,  2014_4  double,  2014_3  double,  2014_2  double,  2014_1  double,  2014_0  double) row format delimited fields terminated by '\t' location '/user/REPLACE_BY_FILE_LOCATION';
```
