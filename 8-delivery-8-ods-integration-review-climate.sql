-- From_staging_yelp, from_staging_weather carries the data transformed from staging to ODS.
-- The join on dates is correct, because by this time I have converted the string date in climate data to valid date format 

select 
    r.dat, 
    r.text,
	r.stars,	
    b.name,
	u.name, 
    p.precipitation, 
    t.min_temp , 
    t.max_temp
from 
    from_staging_yelp.yelp_review r ,
	from_staging_yelp.yelp_user u,
	from_staging_yelp.yelp_business b,
    from_staging_weather.percep p ,
    from_staging_weather.temp t
where
	r.user_id = u.user_id
	and
	r.business_id = b.business_id
	and
    r.dat = p.date_col
    and
    r.dat = t.date_col
LIMIT 10    ;
