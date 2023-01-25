WITH 
D AS 
(SELECT EPCs FROM X
WHERE 
(SELECT epcs_0 FROM Y WHERE reserve_quick_chabert = "RESERVE") IN (epcs_de_zone_1, epcs_de_zone_2, epcs_de_zone_3, epcs_de_zone_4, epcs_de_zone_5, epcs_de_zone_6, epcs_de_zone_7, epcs_de_zone_8, epcs_de_zone_9, epcs_de_zone_10) 
AND 
DATE(datetime_fin) = (SELECT MAX(DATE(datetime_fin)) FROM X WHERE (SELECT epcs_0 FROM Y WHERE reserve_quick_chabert = "RESERVE") IN (epcs_de_zone_1, epcs_de_zone_2, epcs_de_zone_3, epcs_de_zone_4, epcs_de_zone_5, epcs_de_zone_6, epcs_de_zone_7, epcs_de_zone_8, epcs_de_zone_9, epcs_de_zone_10))),

E AS 
(SELECT EPCs FROM X 
WHERE 
(SELECT epcs_0 FROM Y WHERE reserve_quick_chabert = "REBUT") IN (epcs_de_zone_1, epcs_de_zone_2, epcs_de_zone_3, epcs_de_zone_4, epcs_de_zone_5, epcs_de_zone_6, epcs_de_zone_7, epcs_de_zone_8, epcs_de_zone_9, epcs_de_zone_10) 
AND 
DATE(datetime_fin) = (SELECT MAX(DATE(datetime_fin)) FROM X WHERE (SELECT epcs_0 FROM Y WHERE reserve_quick_chabert = "REBUT") IN (epcs_de_zone_1, epcs_de_zone_2, epcs_de_zone_3, epcs_de_zone_4, epcs_de_zone_5, epcs_de_zone_6, epcs_de_zone_7, epcs_de_zone_8, epcs_de_zone_9, epcs_de_zone_10)))

SELECT CURRENT_DATETIME() AS datetime_upload, epcs_0 as EPCs, ean, description,

CASE
  WHEN epcs_0 IN (SELECT EPCs FROM X
WHERE
datetime_fin >= DATE_SUB((SELECT max(datetime_fin) as date FROM X WHERE Statut = "PROPRE"), INTERVAL 40 hour) AND Statut = "PROPRE")
  AND epcs_0 NOT IN (SELECT * FROM reserve)
  AND epcs_0 NOT IN (SELECT * FROM rebut)
  
THEN "A"

  WHEN epcs_0 
  IN (SELECT EPCs FROM X
WHERE
datetime_fin < DATE_SUB((SELECT max(datetime_fin) as date FROM X WHERE Statut = "PROPRE"), INTERVAL 40 hour)
AND
datetime_fin >= DATE_SUB((SELECT max(datetime_fin) as date FROM X WHERE Statut = "PROPRE"), INTERVAL 64 hour) AND Statut = "PROPRE")
  AND epcs_0 NOT IN (SELECT * FROM reserve)
  AND epcs_0 NOT IN (SELECT * FROM rebut)
THEN "B"

  WHEN epcs_0 
  IN (SELECT EPCs FROM X
WHERE
DATETIME(datetime_fin) < DATE_SUB((SELECT max(datetime_fin) as date FROM X WHERE Statut = "PROPRE"), INTERVAL 64 hour) AND Statut = "PROPRE")
  AND epcs_0 NOT IN (SELECT * FROM reserve)
  AND epcs_0 NOT IN (SELECT * FROM rebut)
THEN "C"

  WHEN
  epcs_0 IN (SELECT * FROM reserve)
  AND epcs_0 NOT IN (SELECT * FROM rebut)
THEN "D"

WHEN epcs_0 NOT IN (SELECT * FROM reserve)
  AND epcs_0 IN (SELECT * FROM rebut)
THEN "E"

  WHEN epcs_0
  NOT IN (SELECT EPCs FROM X)
THEN "F"

  ELSE "F"
   
  END as activity

  FROM Y
