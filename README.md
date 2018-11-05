# RBDAProject

## TODOList
* 爬Yelp Rating
* Twitter
* github
* business+user: schema, command, basic profile -> github
* review+checkin+tip: schema, command, basic profile -> github
* join
    * business checkin
    * business review
    * business government
    * user tip
    * business tip
    * user review
* clean
    * business
    * review


## command - create table for business

```sql
create external table business (hours_Monday string, hours_Thursday string, hours_Friday string, hours_Wednesday string, hours_Tuesday string, hours_Saturday string, address string, city string, is_open string, latitude double, review_count int, stars double, name string, attributes_BikeParking string, attributes_BusinessAcceptsCreditCards string, attributes_RestaurantsDelivery string, attributes_BusinessParking_garage string, attributes_BusinessParking_street string, attributes_BusinessParking_validated string, attributes_BusinessParking_lot string, attributes_BusinessParking_valet string, attributes_NoiseLevel string, attributes_RestaurantsGoodForGroups string, attributes_RestaurantsPriceRange2 string, attributes_RestaurantsReservations string, attributes_RestaurantsTakeOut string, attributes_GoodForKids string, attributes_RestaurantsAttire string, attributes_HasTV string, attributes_OutdoorSeating string, neighborhood string, state string, categories string, postal_code string, business_id string, longitude double, hours_Sunday string, attributes_RestaurantsTableService string, attributes_Alcohol string, attributes_WheelchairAccessible string, attributes_Caters string, attributes_DriveThru string, attributes_DogsAllowed string, attributes_WiFi string, attributes_GoodForMeal_dessert string, attributes_GoodForMeal_latenight string, attributes_GoodForMeal_lunch string, attributes_GoodForMeal_dinner string, attributes_GoodForMeal_breakfast string, attributes_GoodForMeal_brunch string, attributes_Ambience_romantic string, attributes_Ambience_intimate string, attributes_Ambience_classy string, attributes_Ambience_hipster string, attributes_Ambience_touristy string, attributes_Ambience_trendy string, attributes_Ambience_upscale string, attributes_Ambience_casual string, hours string, attributes string, attributes_Ambience_divey string, attributes_BestNights_monday string, attributes_BestNights_tuesday string, attributes_BestNights_friday string, attributes_BestNights_wednesday string, attributes_BestNights_thursday string, attributes_BestNights_sunday string, attributes_BestNights_saturday string, attributes_GoodForDancing string, attributes_CoatCheck string, attributes_HappyHour string, attributes_BYOB string, attributes_Music_dj string, attributes_Music_background_music string, attributes_Music_no_music string, attributes_Music_karaoke string, attributes_Music_live string, attributes_Music_video string, attributes_Music_jukebox string, attributes_BYOBCorkage string, attributes_Corkage string, attributes_Smoking string, attributes_ByAppointmentOnly string, attributes_HairSpecializesIn_coloring string, attributes_HairSpecializesIn_africanamerican string, attributes_HairSpecializesIn_curly string, attributes_HairSpecializesIn_perms string, attributes_HairSpecializesIn_kids string, attributes_HairSpecializesIn_extensions string, attributes_HairSpecializesIn_asian string, attributes_HairSpecializesIn_straightperms string, attributes_AcceptsInsurance string, attributes_BusinessAcceptsBitcoin string, attributes_AgesAllowed string, attributes_RestaurantsCounterService string, attributes_Open24Hours string, attributes_DietaryRestrictions_dairy_free string, attributes_DietaryRestrictions_gluten_free string, attributes_DietaryRestrictions_vegan string, attributes_DietaryRestrictions_kosher string, attributes_DietaryRestrictions_halal string, attributes_DietaryRestrictions_soy_free string, attributes_DietaryRestrictions_vegetarian string) row format delimited fields terminated by '\t' location '/user/jl10005/project/business'
```


## command - create table for user

create external table user (compliment_more string, compliment_writer string, compliment_funny string, average_stars string, cool string, review_count string, compliment_plain string, friends string, compliment_note string, fans string, elite string, compliment_profile string, user_id string, yelping_since string, compliment_hot string, name string, compliment_photos string, compliment_cool string, compliment_list string, compliment_cute string, useful string, funny string) row format delimited fields terminated by '\t' location '/user/jl10005/project/user'
