--###############################################
--###############################################
--CREATE STAR SCHEMA QUERIES
--###############################################
--###############################################

-- Create tip dimesnion
CREATE TABLE tip_dim
AS SELECT DISTINCT
	TIP_ID,
	TEXT
FROM
	"WEATHER_RESTAURANTS_ODS"."FROM_STAGING_YELP"."YELP_TIP";
	
CREATE TABLE review_dim
AS SELECT DISTINCT
	REVIEW_ID,
	TEXT
FROM
	"WEATHER_RESTAURANTS_ODS"."FROM_STAGING_YELP"."YELP_REVIEW";	
		

--Create a user dimension
CREATE TABLE user_dim
AS SELECT DISTINCT
    u.USER_ID             , 
    u.NAME                ,
    u.REVIEW_COUNT        ,
    u.YELPING_SINCE       ,
    u.FRIENDS             ,
    u.USEFUL              ,
    u.FUNNY               ,
    u.COOL                ,
    u.FANS                ,
    u.ELITE               ,
    u.AVERGAE_STARS       ,
    u.COMPLIMENT_HOT      ,
    u.COMPLIMENT_MORE     ,
    u.COMPLIMENT_PROFILE  ,
    u.COMPLIMENT_CUTE     ,
    u.COMPLIMENT_LIST     ,
    u.COMPLIMENT_NOTE     ,
    u.COMPLIMENT_PLAIN    ,
    u.COMPLIMENT_COOL     ,
    u.COMPLIMENT_FUNNY    ,
    u.COMPLIMENT_WRITER   ,
    u.COMPLIMENT_PHOTOS  
FROM 
    "WEATHER_RESTAURANTS_ODS"."FROM_STAGING_YELP"."YELP_USER" u;



--Create a business dimesnion
CREATE TABLE business_dim
AS SELECT  DISTINCT
    b.BUSINESS_ID,
    b.NAME,
    b.ADDRESS,
    b.CITY,
    b.STATE,
    b.POSTAL_CODE,
    b.LATITUDE,
    b.LONGITUDE,
    b.STARS,
    b.REVIEW_COUNT,
    b.IS_OPEN,
    b.ATTRIBUTES,
    b.CATEGORIES,
    b.HOURS,
    ch.checkin_date,	
    c.CTA_ENABLED,
    c.COVID_BANNER,
    c.GRUBHUB_ENABLED,
    c.REQUEST_QUOTE_ENALED,
    c.TEMP_CLOSED_UNTIL,
    c.VIRTUAL_SERVICES_OFFERED,
    c.DELIVERY_OR_TAKEOUT,
    c.HIGHLIGHTS
FROM
    "WEATHER_RESTAURANTS_ODS"."FROM_STAGING_YELP"."YELP_BUSINESS" b,
    "WEATHER_RESTAURANTS_ODS"."FROM_STAGING_YELP"."YELP_CHECKIN" ch,	
    "WEATHER_RESTAURANTS_ODS"."FROM_STAGING_YELP"."YELP_COVID" c
WHERE 
    b.business_id=c.business_id
	and
    b.business_id=ch.business_id;


--Create a date dimesnion
CREATE TABLE date_dim
    AS SELECT
        dat
    FROM
        "WEATHER_RESTAURANTS_ODS"."FROM_STAGING_YELP"."YELP_REVIEW"
UNION
    SELECT 
        date
    FROM
        "WEATHER_RESTAURANTS_ODS"."FROM_STAGING_YELP"."YELP_TIP"
UNION
    SELECT
        yelping_since
    FROM
        "WEATHER_RESTAURANTS_ODS"."FROM_STAGING_YELP"."YELP_USER"
UNION
    SELECT
        CHECKIN_DATE
    FROM
        "WEATHER_RESTAURANTS_ODS"."FROM_STAGING_YELP"."YELP_CHECKIN"
UNION
    SELECT
        date_col
    FROM
        "WEATHER_RESTAURANTS_ODS"."FROM_STAGING_WEATHER"."PERCEP"
UNION
    SELECT
       date_col
    FROM
        "WEATHER_RESTAURANTS_ODS"."FROM_STAGING_WEATHER"."TEMP";
        
-- Adding PRIMARY KEY constraints to DIM tables
alter table public.date_dim add PRIMARY KEY (dat) ;
alter table public.business_dim add PRIMARY KEY (business_id);
alter table public.user_dim add PRIMARY KEY (user_id);
alter table public.tip_dim add PRIMARY KEY (tip_id);
alter table public.review_dim add PRIMARY KEY (review_id);


--Create reviews fact
CREATE OR REPLACE SEQUENCE rev_fact_seq;
CREATE TABLE REVIEW_FACT
(
    rev_fact_id			 BIGINT	     primary key
	REVIEW_ID            VARCHAR     FOREIGN KEY  REFERENCES REVIEW_DIM ( REVIEW_ID )  ,
    USER_ID              VARCHAR     FOREIGN KEY  REFERENCES USER_DIM ( USER_ID )           ,
    BUSINESS_ID          VARCHAR     FOREIGN KEY  REFERENCES BUSINESS_DIM ( BUSINESS_ID )   ,
    STARS                FLOAT                             ,
    DAT                  DATE        FOREIGN KEY REFERENCES DATE_DIM ( DAT)      ,
    USEFUL               BIGINT                     ,
    FUNNY                BIGINT                      ,
    COOL                 BIGINT                       ,
	PRECIPITATION        FLOAT                     ,
	PRECIPITATION_NORMAL FLOAT              ,
    MIN_TEMP             FLOAT                          ,
    MAX_TEMP             FLOAT                          ,
    NORMAL_MIN           FLOAT                        ,
    NORMAL_MAX           FLOAT                        
)
AS SELECT DISTINCT
	rev_fact_seq.NEXTVAL,
	R.REVIEW_ID ,          
	R.USER_ID  ,           
    R.BUSINESS_ID    ,     
    R.STARS   ,            
    R.DAT DATE  ,         
    R.USEFUL  ,            
    R.FUNNY   ,            
    R.COOL    ,            
    P.PRECIPITATION     ,  
    P.PRECIPITATION_NORMAL,
    T.MIN_TEMP      ,      
    T.MAX_TEMP   ,         
    T.NORMAL_MIN   ,       
    T.NORMAL_MAX
FROM
	WEATHER_RESTAURANTS_ODS.from_staging_yelp.yelp_review R ,
    WEATHER_RESTAURANTS_ODS.from_staging_weather.percep P ,
    WEATHER_RESTAURANTS_ODS.from_staging_weather.temp T
WHERE
	r.dat = DATE(p.date_col::varchar, 'YYYYMMDD')
    and
    r.dat = DATE(t.date_col::varchar, 'YYYYMMDD');

--create tip fact
--will create a sequence to act as a primary key

CREATE OR REPLACE SEQUENCE tip_fact_seq;


CREATE TABLE TIP_FACT
(	
	tip_fact_id			 BIGINT	     PRIMARY KEY,
	TIP_ID				 BIGINT      FOREIGN KEY REFERENCES TIP_DIM ( TIP_ID)
    DAT                  DATE  FOREIGN KEY REFERENCES DATE_DIM ( DAT),
    COMPLIMENT_COUNT     NUMBER(38,0),
    BUSINESS_ID          VARCHAR FOREIGN KEY  REFERENCES BUSINESS_DIM ( BUSINESS_ID ),
    USER_ID              VARCHAR FOREIGN KEY  REFERENCES USER_DIM ( USER_ID ),
	PRECIPITATION        FLOAT                     ,
	PRECIPITATION_NORMAL FLOAT              ,
    MIN_TEMP             FLOAT                          ,
    MAX_TEMP             FLOAT                          ,
    NORMAL_MIN           FLOAT                        ,
    NORMAL_MAX           FLOAT                        
)   
AS SELECT DISTINCT
	tip_fact_seq.NEXTVAL,
	TIP_ID				,
	DAT                 ,
    COMPLIMENT_COUNT    ,
    BUSINESS_ID         ,
    USER_ID             ,
    PRECIPITATION       ,
    PRECIPITATION_NORMAL,
    MIN_TEMP            ,
    MAX_TEMP            ,
    NORMAL_MIN          ,
    NORMAL_MAX          
 
FROM
	WEATHER_RESTAURANTS_ODS.from_staging_yelp.yelp_TIP T ,
    WEATHER_RESTAURANTS_ODS.from_staging_weather.percep P ,
    WEATHER_RESTAURANTS_ODS.from_staging_weather.temp TMP
WHERE
	T.date = DATE(p.date_col::varchar, 'YYYYMMDD')
    and
    T.date = DATE(TMP.date_col::varchar, 'YYYYMMDD');
	
	