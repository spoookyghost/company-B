WITH 
fraude_statistics AS 

(SELECT ROW_NUMBER() OVER(ORDER BY boxTypeId, year_month) id, boxTypeId, year_month, SUM(count) AS sum, ROUND(AVG(count), 2) AS average, ROUND(STDDEV(count), 2) AS stddev, ROUND(AVG(count) + 3 * STDDEV(count), 2) AS avgplus3stddev FROM 

(SELECT itemId,  UserId, year_month, boxTypeId, COUNT(transactionItemsId) AS count FROM

(SELECT itemId, UserId, boxTypeId, boxTypeName, transactionItemsId, transactionCreatedAt, DATE_TRUNC(DATE(transactionCreatedAt), MONTH) AS year_month 
FROM A)

WHERE Userid IS NOT NULL AND itemId IS NOT NULL AND boxTypeName NOT LIKE "%OLD%"

GROUP BY itemId, UserId, year_month, boxTypeId

ORDER BY itemId)

GROUP BY boxTypeId, year_month

ORDER BY boxTypeId), 

fraude_identification AS

(SELECT ROW_NUMBER() OVER(ORDER BY itemId, UserId) id, itemId, UserId, transactionUserIdentifier, group_by_month.year_month, group_by_month.boxTypeId, CAST(mapping.x__client_id AS STRING) AS x__client_id, ItemValue, count, average AS average_for_same_company_and_period, stddev AS stddev_for_same_company_and_period, avgplus3stddev AS avgplus3stddev_for_same_company_and_period,
CASE 
WHEN count > avgplus3stddev THEN "YES"
WHEN count <= avgplus3stddev THEN "NO"
ELSE "NO"
END AS fraude_suspicion, 
CAST(group_by_month.boxTypeId AS STRING) AS boxTypeIdX

FROM (SELECT itemId, UserId, transactionUserIdentifier, year_month, boxTypeId, ItemValue, COUNT(transactionItemsId) AS count FROM

(SELECT itemId, UserId, transactionUserIdentifier, boxTypeId, boxTypeName, transactionItemsId, transactionCreatedAt, transactionItemValue AS ItemValue, DATE_TRUNC(DATE(transactionCreatedAt), MONTH) AS year_month 
FROM A)

WHERE Userid IS NOT NULL AND itemId IS NOT NULL AND boxTypeName NOT LIKE "%OLD%"

GROUP BY itemId, UserId, transactionUserIdentifier, year_month, boxTypeId, ItemValue) AS group_by_month

LEFT JOIN fraude_statistics

ON group_by_month.boxTypeId = fraude_statistics.boxTypeId

AND group_by_month.year_month = fraude_statistics.year_month

LEFT JOIN B AS mapping
ON
group_by_month.boxTypeId = CAST(mapping.boxTypeId AS INT))

SELECT *,
CASE
WHEN fraude_suspicion = "YES" THEN ROUND(count - average_for_same_company_and_period) * ItemValue
ELSE NULL
END AS fraude_estimation
FROM fraude_identification
