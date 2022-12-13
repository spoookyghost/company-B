import pandas as pd
import numpy as np
from pathlib import Path  
import os

# Define the local path
cwd = os.path.abspath(r"X")
file_list = os.listdir(cwd)

df_merged = pd.DataFrame()

# Read, parse and merge every csv file in the folder
for file in file_list:
            
    address = "Y"
    
    file_address = address + file
    
    original_df = pd.read_csv(file_address, header = None)
    
    # Process the metadata of the file
    metadata = original_df.head(2)

    metadata.rename(columns={metadata.columns[0]: "X" }, inplace = True)

    metadata.reset_index(inplace = True)

    metadata.drop(columns = {"index"})

    # Parse the metadata using lists
    l1_first_part = list()
    l1_second_part = list()   
    
    for i in range(len(metadata)):
        row_value = metadata["X"][i]
        row_value_split_in_list_first_part = row_value.split(";")
        first_part = row_value_split_in_list_first_part[:6]
        row_value_split_in_list_second_part = row_value.split('"')
        row_value_split_in_list_second_part = str(row_value_split_in_list_second_part).split(",")
        second_part = row_value_split_in_list_second_part[1:]
        l1_first_part.append(first_part)
        l1_second_part.append(second_part[0:1])

        if not any(l1_second_part):
            l = list()
            f1 = list()
            f1.append("epcs de zone")
            f2 = list()
            f2.append("")
            l.append(f1)
            l.append(f2)
            final_list = [x + y for x, y in zip(l1_first_part, l)]
            
        else:
            final_list = [x + y for x, y in zip(l1_first_part, l1_second_part)]
            
    df1 = pd.DataFrame(final_list)
    df1 = df1.rename(columns= df1.iloc[0]).loc[1:]
    df1.rename(columns={df1.columns[6]: "epcs de zone" }, inplace = True)
    
    # Process the data of the file
    data = original_df.iloc[2: , :]

    data.rename(columns={data.columns[0]: "X"}, inplace = True)

    data.reset_index(inplace = True)

    data.drop(columns = {"index"})
    
    # Parse the data using lists
    l2 = list()

    for i in range(len(data)):
        row_value = data["X"][i]
        row_value_split_in_list = row_value.split(";")
        row_value_split_in_list = row_value_split_in_list[0:3]
        l2.append(row_value_split_in_list)

    # Combine the metadata and data in one dataframe    
    df2 = pd.DataFrame(l2)
    df2 = df2.rename(columns= df2.iloc[0]).loc[1:]
    
    df3 = df2.copy()
    
    df3["type"] = df1["type"].iat[0]

    df3["date"] = df1["date"].iat[0]

    df3["heure debut"] = df1["heure debut"].iat[0]

    df3["heure fin"] = df1["heure fin"].iat[0]
    
    df3["epcs de zone"] = df1["epcs de zone"].iat[0]
        
    df3["heure debut"] = df3["heure debut"].str.replace("h", ":")    
    
    df3["datetime debut"] = df3["date"] + " " + df3["heure debut"]

    df3["datetime debut"] = pd.to_datetime(df3["datetime debut"], dayfirst=True)
                                      
    df3["heure fin"] = df3["heure fin"].str.replace("h", ":")
    
    df3["datetime fin"] = df3["date"] + " " + df3['heure fin']

    df3["datetime fin"] = pd.to_datetime(df3["datetime fin"], dayfirst=True)
    
    df3["epcs de zone"] = df3["epcs de zone"].str.replace("'", "")
    
    df3 = df3.drop(columns = {"date", "heure debut", "heure fin"})
    
    df_merged = pd.concat([df_merged, df3])
    
    df_merged = df_merged.reset_index(drop=True)
    
    # Transform the dataframe into the "joined_file.csv"
df_merged.to_csv("joined_file.csv", index = False)
