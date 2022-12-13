import pandas as pd
import numpy as np
import pandasql
from datetime import date, timedelta
pd.set_option("display.max_rows", 200)

# Read the file downloaded from the table "X"
X = pd.read_csv(r"X.csv", sep = ",")

# Read the file downloaded from the table "Y"
Y = pd.read_excel(r"Y.xlsx")

# Create a dataframe will datetimes at 4 am, ranging from the first inventory to the last inventory
date_range = pd.DataFrame({"date": pd.date_range(X["datetime_fin"].min(),X["datetime_fin"].max()
                                                 , freq="D")})
date_range["date"] = pd.to_datetime(date_range["date"]).dt.normalize() + timedelta(hours = 4)

date_range = date_range[(date_range["date"] > "2022-08-08 04:00:00")]

date_range.reset_index(inplace = True, drop = True)

df_merged = pd.DataFrame()

# There is one query per date, the resulting queries are merged together
for i in range(len(date_range)):
        
    q1 = """
SELECT epcs_0 as EPCs, ean, description, effectiveAt, 
CASE WHEN epcs_0 
IN 
(SELECT EPCs FROM X WHERE Statut = "?" 
AND DATETIME(datetime_fin) BETWEEN DATETIME('{}', "-2 day") AND DATETIME('{}'))
THEN "?"
  WHEN epcs_0 
IN 
(SELECT EPCs FROM X
WHERE
DATETIME(datetime_fin) BETWEEN DATETIME('{}', "-3 day") AND DATETIME('{}', "-2 day")
AND Statut == "?") 
THEN "?"
  WHEN epcs_0 
IN 
(SELECT EPCs FROM X
WHERE
DATETIME(datetime_fin) < DATETIME('{}', "-3 day")) 
THEN "?"
    
WHEN epcs_0
NOT IN (SELECT EPCs FROM X)
  
THEN "?"
ELSE "?"
END as activity
FROM Y
""".format(date_range["date"][i], date_range["date"][i], date_range["date"][i], date_range["date"][i], date_range["date"][i], date_range["date"][i])

    sub_data = pandasql.sqldf(q1, globals())
    sub_data["datetime_comparison"] = date_range["date"][i]
    df_merged = pd.concat([df_merged, sub_data])

df_merged = df_merged.reset_index(drop=True)

df_merged = df_merged.reset_index(level=0)

# Transform the dataframe into the "chips_BK_with_activity_partitioned.csv"
df_merged.to_csv("Y.csv", index = False)
