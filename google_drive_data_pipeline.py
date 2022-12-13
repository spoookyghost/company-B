import pandas as pd
import numpy as np
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import gspread
from gspread_dataframe import set_with_dataframe

# Connect to the Google Drive API
API_KEY = "?"

drive_service = build("drive", "v3", developerKey = API_KEY)

# Connect to the Google Sheets API
gc = gspread.service_account(filename = "credentials.json")

# Read the "database" Google Sheets
sheet = gc.open_by_key("X")

row=1

col=1

worksheet = sheet.get_worksheet(0)

# Function to get the id of all the files in a Google Drive folder
def printChildren(parent):
    id_list = list()
    
    param = {"q": "'" + parent + "' in parents and mimeType != 'application/vnd.google-apps.folder'"}
    result = drive_service.files().list(**param).execute()
    files = result.get("files")

    for afile in files:
        id_list.append(afile.get("id"))
    
    return id_list

# Read the "id_list_of_uploaded_files_BK" Google Sheets
sheet = gc.open_by_key("Y")

row=1

col=1

id_uploaded = sheet.get_worksheet(0)

df_id_uploaded = pd.DataFrame(id_uploaded.get_all_records())

id_uploaded_list = df_id_uploaded.values.tolist()

id_uploaded_list = [item[0] for item in id_uploaded_list]

df_merged = pd.DataFrame()

# List of all the files in the folder "Data"
id_to_upload_list = printChildren("Z")

for id in id_to_upload_list:
    
    # Different condition if the id is the one of the "database BK"
    if id == "X":
        
        database = pd.DataFrame(worksheet.get_all_records())
    
    # For all the other csv files whose id is not the list of the uploaded files
    if id not in id_uploaded_list:
        
        # Read the csv
        csv_file_url = "https://drive.google.com/file/d/" + id + "/view?usp=sharing"

        path = "https://drive.google.com/uc?export=download&id=" + csv_file_url.split("/")[-2]
    
        original_df = pd.read_csv(path, header = None)
    
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
                f2.append("Null")
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
        
# Combine the data to upload with the previously uploaded data and push it to Google Sheets
df_to_push = pd.concat([df_merged, database])

worksheet.clear()

set_with_dataframe(worksheet, df_to_push, row, col)

# Update the list of the id of the files to upload and push it to Google Sheets
df_ids = pd.DataFrame(id_to_upload_list, columns=["ids"])

id_uploaded.clear()

set_with_dataframe(id_uploaded, df_ids, row, col)
