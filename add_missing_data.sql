WITH ventes_cpl_year_month AS (
SELECT boxTypeId, year_month, SUM(Nombre_Ventes_Consignes) AS Nombre_Ventes_Consignes FROM 
(SELECT BoxId AS boxTypeId, DATE_TRUNC(DATE(Date), MONTH) AS year_month, Nombre_Ventes_Consignes  FROM A) 
GROUP BY boxTypeId, year_month),

transactions_data_group_by_year_month AS (SELECT boxTypeId, year_month, COUNT(transactionItemsId) AS count FROM

(SELECT itemId, UserId, boxTypeId, boxTypeName, transactionItemsId, transactionCreatedAt, DATE_TRUNC(DATE(transactionCreatedAt), MONTH) AS year_month 
FROM B)

WHERE itemId IS NOT NULL AND boxTypeName NOT LIKE "%OLD%"

GROUP BY boxTypeId, year_month)

SELECT transactions_data_group_by_year_month.boxTypeId AS boxTypeId, transactions_data_group_by_year_month.year_month, Nombre_Ventes_Consignes, count, CAST(mapping.x__client_id AS STRING) AS x__client_id, CAST(transactions_data_group_by_year_month.boxTypeId AS STRING) AS boxTypeIdX  FROM transactions_data_group_by_year_month

LEFT JOIN ventes_cpl_year_month

ON ventes_cpl_year_month.boxTypeId = transactions_data_group_by_year_month.boxTypeId

AND ventes_cpl_year_month.year_month = transactions_data_group_by_year_month.year_month

LEFT JOIN C AS mapping

ON transactions_data_group_by_year_month.BoxTypeId = CAST(mapping.boxTypeId AS INT)

WHERE Nombre_Ventes_Consignes IS NOT NULL AND Nombre_Ventes_Consignes != 0
