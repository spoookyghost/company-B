SELECT ROW_NUMBER() OVER(ORDER BY datetime_upload) id, * FROM(SELECT CURRENT_DATETIME() AS datetime_upload, epcs_0 as EPCs, ean, description,

CASE 
  WHEN epcs_0
  IN (SELECT EPCs FROM `X`
WHERE
datetime_fin >= DATE_SUB((SELECT max(datetime_fin) as date FROM `X` WHERE Statut = "?"), INTERVAL 2 DAY))
  AND epcs_0 NOT IN
(SELECT EPCs FROM `X`
WHERE epcs_de_zone_1 IN (SELECT epcs_0 FROM `Y` WHERE epcs_0 = "?")
OR epcs_de_zone_2 IN (SELECT epcs_0 FROM `Y` WHERE epcs_0 = "?")
OR epcs_de_zone_3 IN (SELECT epcs_0 FROM `Y` WHERE epcs_0 = "?")
OR epcs_de_zone_4 IN (SELECT epcs_0 FROM `Y` WHERE epcs_0 = "?")
OR epcs_de_zone_5 IN (SELECT epcs_0 FROM `Y` WHERE epcs_0 = "?")
OR epcs_de_zone_6 IN (SELECT epcs_0 FROM `Y` WHERE epcs_0 = "?"))
  
THEN "?"

  WHEN epcs_0 
  IN (SELECT EPCs FROM `X`
WHERE
datetime_fin < DATE_SUB((SELECT max(datetime_fin) as date FROM `X` WHERE Statut = "?"), INTERVAL 2 DAY)
AND
datetime_fin >= DATE_SUB((SELECT max(datetime_fin) as date FROM `X` WHERE Statut = "?"), INTERVAL 3 DAY)) 
THEN "?"

  WHEN epcs_0 
  IN (SELECT EPCs FROM `X`
WHERE
DATETIME(datetime_fin) < DATE_SUB((SELECT max(datetime_fin) as date FROM `X` WHERE Statut = "?"), INTERVAL 3 DAY)) 
THEN "?"

  WHEN epcs_0
  NOT IN (SELECT EPCs FROM `X`)
  OR epcs_0
  IN
  (SELECT EPCs FROM `X`
WHERE epcs_de_zone_1 IN (SELECT epcs_0 FROM `Y` WHERE epcs_0 = "?")
OR epcs_de_zone_2 IN (SELECT epcs_0 FROM `Y` WHERE epcs_0 = "?")
OR epcs_de_zone_3 IN (SELECT epcs_0 FROM `Y` WHERE epcs_0 = "?")
OR epcs_de_zone_4 IN (SELECT epcs_0 FROM `Y` WHERE epcs_0 = "?")
OR epcs_de_zone_5 IN (SELECT epcs_0 FROM `Y` WHERE epcs_0 = "?")
OR epcs_de_zone_6 IN (SELECT epcs_0 FROM `Y` WHERE epcs_0 = "?"))
THEN "?"

  ELSE "?"
   
  END as activity

  FROM `Z`)
