--###########################################
--###########################################
--creating the correlation report
--###########################################
--###########################################

SELECT DISTINCT
	RD.TEXT AS REVIEW_TEXT
	RF.STARS AS STARS, 
	RF.MIN_TEMP AS MIN_TEMP, 
	RF.MAX_TEMP AS MAX_TEMP, 
	RF.PRECIPITATION AS PRECIPITATION, 
	B.NAME AS BUSINESS_NAME
FROM
	REVIEW_FACT RF,
	REVIEW_DIM RD,
	BUSINESS_DIM B
WHERE
	RF.REVIEW_ID = RD.REVIEW_ID
	and
	RF.BUSINESS_ID = B.BUSINESS_ID;	