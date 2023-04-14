CREATE OR REPLACE TABLE FUNCTION A.B(resto STRING)
AS (WITH 
E  AS 
(SELECT EPCs FROM `C`
WHERE 
(SELECT epcs_0 FROM `D` WHERE type = "E " AND restaurant = resto) IN (epcs_de_zone_1, epcs_de_zone_2, epcs_de_zone_3, epcs_de_zone_4, epcs_de_zone_5, epcs_de_zone_6, epcs_de_zone_7, epcs_de_zone_8, epcs_de_zone_9, epcs_de_zone_10)
AND 
DATE(datetime_fin) = (SELECT MAX(DATE(datetime_fin)) FROM `B` WHERE (SELECT epcs_0 FROM `C` WHERE type = "E " AND restaurant = resto) IN (epcs_de_zone_1, epcs_de_zone_2, epcs_de_zone_3, epcs_de_zone_4, epcs_de_zone_5, epcs_de_zone_6, epcs_de_zone_7, epcs_de_zone_8, epcs_de_zone_9, epcs_de_zone_10)) 
AND restaurant = resto),

F AS 
(SELECT EPCs FROM `B`  
WHERE 
(SELECT epcs_0 FROM `C` WHERE type = "REBUT" AND restaurant = resto) IN (epcs_de_zone_1, epcs_de_zone_2, epcs_de_zone_3, epcs_de_zone_4, epcs_de_zone_5, epcs_de_zone_6, epcs_de_zone_7, epcs_de_zone_8, epcs_de_zone_9, epcs_de_zone_10) 
AND 
DATE(datetime_fin) = (SELECT MAX(DATE(datetime_fin)) FROM `B` WHERE (SELECT epcs_0 FROM `C` WHERE type = "REBUT" AND restaurant = resto) IN (epcs_de_zone_1, epcs_de_zone_2, epcs_de_zone_3, epcs_de_zone_4, epcs_de_zone_5, epcs_de_zone_6, epcs_de_zone_7, epcs_de_zone_8, epcs_de_zone_9, epcs_de_zone_10)) 
AND restaurant = resto),

G AS 
(SELECT
EPCs, datetime_fin
FROM (SELECT ROW_NUMBER () OVER (PARTITION BY EPCs ORDER BY datetime_fin DESC) AS row_number, EPCs, datetime_fin FROM `B` 
WHERE restaurant = resto)
WHERE row_number = 1),

H AS (SELECT EPCs, datetime_fin FROM `B`  
WHERE 
(SELECT epcs_0 FROM `C` WHERE type = "REBUT" AND restaurant = resto) IN (epcs_de_zone_1, epcs_de_zone_2, epcs_de_zone_3, epcs_de_zone_4, epcs_de_zone_5, epcs_de_zone_6, epcs_de_zone_7, epcs_de_zone_8, epcs_de_zone_9, epcs_de_zone_10)
AND restaurant = resto),

I AS (SELECT rebut.EPCs AS EPCs FROM rebut
LEFT JOIN last_inventory
ON rebut.EPCs = last_inventory.EPCs
AND rebut.datetime_fin = last_inventory.datetime_fin
WHERE last_inventory.EPCs IS NOT NULL)

SELECT CURRENT_DATETIME() AS datetime_upload, epcs_0 as EPCs, ean, description, SITE,

CASE
  WHEN epcs_0 IN (SELECT EPCs FROM I) AND epcs_0 NOT IN (SELECT E )

THEN “Z”

  WHEN epcs_0 IN (SELECT EPCs FROM `B`
WHERE
datetime_fin >= DATE_SUB((SELECT max(datetime_fin) as date FROM `B` WHERE Statut = "PROPRE" AND restaurant = resto), INTERVAL 40 hour) AND Statut = "PROPRE" AND restaurant = resto)
  AND epcs_0 NOT IN (SELECT * FROM E )
  AND epcs_0 NOT IN (SELECT * FROM F)
  
THEN “Y”

  WHEN epcs_0 
  IN (SELECT EPCs FROM `B`
WHERE
datetime_fin < DATE_SUB((SELECT max(datetime_fin) as date FROM `B` WHERE Statut = "PROPRE" AND restaurant = resto), INTERVAL 40 hour)
AND
datetime_fin >= DATE_SUB((SELECT max(datetime_fin) as date FROM `B` WHERE Statut = "PROPRE" AND restaurant = resto), INTERVAL 64 hour) AND Statut = "PROPRE" AND restaurant = resto)
  AND epcs_0 NOT IN (SELECT * FROM E )
  AND epcs_0 NOT IN (SELECT * FROM F)
THEN “X”

  WHEN epcs_0 
  IN (SELECT EPCs FROM `B`
WHERE
DATETIME(datetime_fin) < DATE_SUB((SELECT max(datetime_fin) as date FROM `B` WHERE Statut = "PROPRE" AND restaurant = resto), INTERVAL 64 hour) AND Statut = "PROPRE" AND restaurant = resto)
  AND epcs_0 NOT IN (SELECT * FROM E )
  AND epcs_0 NOT IN (SELECT * FROM F)
THEN “W”

  WHEN
  epcs_0 IN (SELECT * FROM E )
  AND epcs_0 NOT IN (SELECT * FROM F)
THEN “V”

  WHEN epcs_0
  NOT IN (SELECT EPCs FROM `B` WHERE restaurant = resto)
THEN “U”

  ELSE “Z”
   
  END as activity

  FROM `D` WHERE SITE = resto)
