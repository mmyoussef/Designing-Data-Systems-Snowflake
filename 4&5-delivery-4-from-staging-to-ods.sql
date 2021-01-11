--##################################################
--##################################################
--COPY STATEMENTS TO LOAD THE DATA TO STAGING TABLES
--##################################################
--##################################################

copy into WEATHER.percept from @weather_restauarnt_stg/percep.csv.gz FILE_FORMAT=(TYPE = CSV, COMPRESSION=GZIP, SKIP_HEADER = 1);


copy into WEATHER.percept from @weather_restauarnt_stg/percep.csv.gz FILE_FORMAT=(TYPE = CSV, COMPRESSION=GZIP, SKIP_HEADER = 1);

copy into yelp.YELP_covid from @weather_restaurants.yelp.weather_restauarnt_stg/yelp_academic_dataset_covid_features.json.gz  File_format=(type=JSON);

copy into yelp.YELP_USER from @weather_restaurants.yelp.weather_restauarnt_stg pattern='.*user.*'  File_format=(type=JSON) ON_ERROR=CONTINUE;

copy into yelp.YELP_tip from @weather_restaurants.yelp.weather_restauarnt_stg pattern='.*tip.*'  File_format=(type=JSON);

copy into yelp.YELP_REVIEW from @weather_restaurants.yelp.weather_restauarnt_stg pattern='.*review.*'  File_format=(type=JSON) ON_ERROR=CONTINUE;

copy into yelp.YELP_checkin from @weather_restaurants.yelp.weather_restauarnt_stg/yelp_academic_dataset_checkin.json.gz  File_format=(type=JSON);

copy into yelp.YELP_BUSINESS from @weather_restaurants.yelp.weather_restauarnt_stg/yelp_academic_dataset_business.json.gz  File_format=(type=JSON);


--###################################################
--###################################################
--TRANSFORMING YELP JSON DATA TO TABLES IN ODS
--###################################################
--###################################################


--BUSINESS DATA
CREATE TABLE WEATHER_RESTAURANTS_ODS.FROM_STAGING_YELP.YELP_BUSINESS
AS select 
	PARSE_JSON(value):business_id::varchar AS business_id, PARSE_JSON(value):name::varchar AS name, 
	PARSE_JSON(value):address::varchar AS address, 
	PARSE_JSON(value):city::varchar AS city, 
	PARSE_JSON(value):state::varchar AS state, 
	PARSE_JSON(value):postal_code::VARCHAR AS postal_code, 
	PARSE_JSON(value):latitude::float AS latitude, 
	PARSE_JSON(value):longitude::float AS longitude,
	PARSE_JSON(value):stars::float AS stars, 
	PARSE_JSON(value):review_count::bigint AS review_count, 
	PARSE_JSON(value):is_open::smallint AS is_open, 
	PARSE_JSON(value):attributes::variant AS attributes, 
	PARSE_JSON(value):categories::variant AS categories, 
	PARSE_JSON(value):hours::variant AS hours
from weather_restaurants.yelp.yelp_business;


--REVIEW DATA
CREATE TABLE WEATHER_RESTAURANTS_ODS.FROM_STAGING_YELP.YELP_REVIEW
AS SELECT
	PARSE_JSON(value):review_id::varchar AS review_id,
	PARSE_JSON(value):user_id::varchar AS user_id,
	PARSE_JSON(value):business_id::varchar AS business_id,
	PARSE_JSON(value):stars::float AS stars,
	PARSE_JSON(value):date::date AS dat,
	PARSE_JSON(value):text::varchar AS text,
	PARSE_JSON(value):useful::BIGINT AS useful,
	PARSE_JSON(value):funny::BIGINT AS funny,
	PARSE_JSON(value):cool::BIGINT AS cool
FROM weather_restaurants.yelp.yelp_review;

--USER DATA
CREATE TABLE WEATHER_RESTAURANTS_ODS.FROM_STAGING_YELP.YELP_USER
AS SELECT	
	PARSE_JSON(value):user_id::varchar AS user_id, 
	PARSE_JSON(value):name::varchar AS name, 
	PARSE_JSON(value):review_count::bigint AS review_count,  
	PARSE_JSON(value):yelping_since::date AS yelping_since, 
	PARSE_JSON(value):friends::variant friends, 
	PARSE_JSON(value):useful::bigint useful,
	PARSE_JSON(value):funny::bigint funny, 
	PARSE_JSON(value):cool::bigint cool, 
	PARSE_JSON(value):fans::bigint fans, 
	PARSE_JSON(value):elite::variant elite, 
	PARSE_JSON(value):avergae_stars::float      avergae_stars, 
	PARSE_JSON(value):compliment_hot::BIGINT    compliment_hot, 
	PARSE_JSON(value):compliment_more::BIGINT   compliment_more, 
	PARSE_JSON(value):compliment_profile::BIGINT compliment_profile,  
	PARSE_JSON(value):compliment_cute::BIGINT compliment_cute, 
	PARSE_JSON(value):compliment_list::BIGINT compliment_list, 
	PARSE_JSON(value):compliment_note::BIGINT compliment_note, 
	PARSE_JSON(value):compliment_plain::BIGINT compliment_plain, 
	PARSE_JSON(value):compliment_cool::BIGINT compliment_cool, 
	PARSE_JSON(value):compliment_funny::BIGINT compliment_funny, 
	PARSE_JSON(value):compliment_writer::BIGINT compliment_writer, 
	PARSE_JSON(value):compliment_photos::BIGINT compliment_photos
FROM weather_restaurants.yelp.yelp_user;

--TIP DATA

CREATE OR REPLACE SEQUENCE tip_seq;

CREATE TABLE WEATHER_RESTAURANTS_ODS.FROM_STAGING_YELP.YELP_TIP 
(
	TIP_ID BIGINT default tip_seq.nextval,
	text varchar,
	date date,
	compliment_count BIGINT,
	business_id varchar,
	user_id varchar
)
	
INSERT INTO  WEATHER_RESTAURANTS_ODS.FROM_STAGING_YELP.YELP_TIP
(
	text ,
	date ,
	compliment_count ,
	business_id ,
	user_id 
)
SELECT
	PARSE_JSON(value):text::varchar text,
	PARSE_JSON(value):date::date date,
	PARSE_JSON(value):compliment_count::BIGINT compliment_count,
	PARSE_JSON(value):business_id::varchar business_id,
	PARSE_JSON(value):user_id::varchar user_id
FROM weather_restaurants.yelp.yelp_TIP;	

--CHECKIN DATA

CREATE TABLE WEATHER_RESTAURANTS_ODS.FROM_STAGING_YELP.YELP_CHECKIN
AS select 
    PARSE_JSON(value):business_id::varchar BUSINESS_ID, 
    PARSE_JSON(value):date CHECKIN_DATE 
from "WEATHER_RESTAURANTS"."YELP"."YELP_CHECKIN"; 

--COVID DATA

CREATE TABLE WEATHER_RESTAURANTS_ODS.FROM_STAGING_YELP.YELP_COVID
AS SELECT
	PARSE_JSON(value):"Call To Action enabled"::BOOLEAN CTA_ENABLED,
	PARSE_JSON(value):"Covid Banner"::VARCHAR COVID_BANNER,
	PARSE_JSON(value):"Grubhub enabled"::BOOLEAN GRUBHUB_ENABLED,
	PARSE_JSON(value):"Request a Quote Enabled"::BOOLEAN REQUEST_QUOTE_ENALED,
	PARSE_JSON(value):"Temporary Closed Until"::VARCHAR TEMP_CLOSED_UNTIL,
	PARSE_JSON(value):"Virtual Services Offered"::VARCHAR VIRTUAL_SERVICES_OFFERED,
	PARSE_JSON(value):"business_id"::VARCHAR BUSINESS_ID,
	PARSE_JSON(value):"delivery or takeout"::BOOLEAN DELIVERY_OR_TAKEOUT,
	PARSE_JSON(value):"highlights"::VARCHAR highlights
FROM weather_restaurants.yelp.yelp_COVID;

--###############################################
--###############################################
--COPYING CLIMATE DATE FROM STAGING TO ODS
--###############################################
--###############################################

--PRECEP DATA

CREATE TABLE PERCEPT (DATE_COL DATE , precipitation FLOAT, precipitation_normal FLOAT)
AS SELECT 
	DATE(p.date_col::varchar, 'YYYYMMDD') DATE_COL, 
	case percepitation when 'T' THEN 0 ELSE PERCEPITATION::FLOAT END, 
	percipitation_normal::FLOAT
FROM "WEATHER_RESTAURANTS"."WEATHER"."PERCEPT" p;


--TEMP DATA

CREATE TABLE TEMP (DATE_COL DATE , MIN_TEMP FLOAT, MAX_TEMP FLOAT, NORMAL_MIN FLOAT, NORMAL_MAX FLOAT)
AS SELECT 
	DATE(t.date_col::varchar, 'YYYYMMDD') DATE_COL,
	MIN_TMP::FLOAT, 
	MAX_TMP::FLOAT, 
	NORMAL_MIN::FLOAT, 
	NORMAL_MAX::FLOAT
FROM "WEATHER_RESTAURANTS"."WEATHER"."TEMP" t;

