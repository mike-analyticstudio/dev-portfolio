# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8af43c6b-4303-4b0b-8459-7621117771f2",
# META       "default_lakehouse_name": "laserengravelakehouse",
# META       "default_lakehouse_workspace_id": "4ecbaf18-d748-4bd3-b6cd-5743f135f2bf",
# META       "known_lakehouses": [
# META         {
# META           "id": "8af43c6b-4303-4b0b-8459-7621117771f2"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# <mark>**Customer Churn Data**</mark>
# 
# This script generates Customer churn data - also called attrition). This data is meant to measure how our company maintains our customer retention. 
# 
# <span style="background-color:pink">Constantly analysing to provide various innovative incentives to retain our existing customers.</span>


# CELL ********************

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta


from notebookutils import mssparkutils  # Only works in Microsoft Fabric

%pip install faker

from faker import Faker

# --- Configuration ---
n = 5000
np.random.seed(42)
random.seed(42)

# Regions and Territories with lat/lon
territories = {
    "North": [
        ("Manchester", 53.4808, -2.2426),
        ("Leeds", 53.8008, -1.5491),
        ("Newcastle", 54.9784, -1.6174),
        ("Liverpool", 53.4084, -2.9916)
    ],
    "South": [
        ("London", 51.5074, -0.1278),
        ("Brighton", 50.8225, -0.1372),
        ("Oxford", 51.7520, -1.2577),
        ("Reading", 51.4543, -0.9781)
    ],
    "East": [
        ("Cambridge", 52.2053, 0.1218),
        ("Norwich", 52.6309, 1.2974),
        ("Ipswich", 52.0567, 1.1482)
    ],
    "West": [
        ("Bristol", 51.4545, -2.5879),
        ("Exeter", 50.7184, -3.5339),
        ("Bath", 51.3758, -2.3599),
        ("Cardiff", 51.4816, -3.1791)
    ]
}

# Segments and channels
segments = ["Enterprise", "SMB", "Individual"]
channels = {
    "Enterprise": ["Email", "Web"],
    "SMB": ["Phone", "Email"],
    "Individual": ["Web", "Phone"]
}

# --- Helper Functions ---
def random_date(start, end):
    delta = end - start
    return start + timedelta(days=np.random.randint(0, delta.days))

def choose_territory(region):
    return random.choice(territories[region])

def choose_segment():
    return np.random.choice(segments, p=[0.2, 0.4, 0.4])

def churn_probability(row):
    base = {"Enterprise": 0.15, "SMB": 0.35, "Individual": 0.45}
    prob = base[row["CustomerSegment"]]
    # Increase if low spend, high inactivity, or many tickets
    if row["TotalSpend"] < 300:
        prob += 0.15
    inactivity = (datetime(2025,10,1) - row["LastPurchaseDate"]).days
    if inactivity > 300:
        prob += 0.20
    if row["SupportTickets"] > 3:
        prob += 0.10
    return min(prob, 0.95)

# --- Generate Data ---
rows = []
start_join = datetime(2018,1,1)
end_join = datetime(2023,1,1)
end_purchase = datetime(2025,10,1)

for i in range(1, n+1):
    cust_id = f"C{i:04d}"
    region = np.random.choice(list(territories.keys()))
    territory, lat, lon = choose_territory(region)
    join_date = random_date(start_join, end_join)
    last_purchase = random_date(join_date, end_purchase)
    total_spend = round(np.random.lognormal(mean=6.5, sigma=0.8), 2)
    support_tickets = min(np.random.poisson(2), 10)
    segment = choose_segment()
    channel = random.choice(channels[segment])
    row = {
        "CustomerID": cust_id,
        "JoinDate": join_date,
        "LastPurchaseDate": last_purchase,
        "TotalSpend": total_spend,
        "SupportTickets": support_tickets,
        "Region": region,
        "Territory": territory,
        "Latitude": lat,
        "Longitude": lon,
        "CustomerSegment": segment,
        "PreferredChannel": channel
    }
    rows.append(row)

df = pd.DataFrame(rows)

# Compute churn
df["ChurnProb"] = df.apply(churn_probability, axis=1)
df["Churned"] = df["ChurnProb"].apply(lambda x: "Yes" if np.random.rand() < x else "No")
df.drop(columns=["ChurnProb"], inplace=True)

# Save to CSV
df.to_csv("fabric_customer_churn_data.csv", index=False)

#convert to spark dataframe
df_spark = spark.createDataFrame(df)

#save to SQL table 
df_spark.write.mode("overwrite").saveAsTable("customer_churn_data")

#I want the file saved in the base 'Files' folder as is - needs a few steps urggg
df_spark.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", True) \
    .csv("Files/tmp_customer_churn_data")

'''
# List the files written to the temp folder
#files = dbutils.fs.ls("Files/tmp_customer_data")

# Find the actual CSV file (skip _SUCCESS or metadata files)

#csv_file = [f.path for f in files if f.path.endswith(".csv")][0]

# Move it to the base Files folder with a proper name

#dbutils is a databricks native module
#dbutils.fs.mv(csv_file, "Files/customer_data.csv")   

#df.to_csv("customer_data.csv", index=False)

#Optional - delete the temporary temp location
#dbutils.fs.rm("Files/tmp_customer_data", recurse=True) 
'''

# List files in the temp folder
files = mssparkutils.fs.ls("Files/tmp_customer_churn_data")    # only a microsoft module

# Find the part file (the real CSV)
csv_file_path = [f.path for f in files if f.path.endswith(".csv")][0]


# Source and destination paths
source_path = "Files/tmp_customer_churn_data"
dest_path = "Files/customer_churn_data.csv"

# Delete existing destination if it exists
if mssparkutils.fs.exists(dest_path):
    print(f"Deleting existing file at {dest_path} ...")
    mssparkutils.fs.rm(dest_path, recurse=True)

# Move and rename to customer_data.csv in the root Files folder
mssparkutils.fs.mv(csv_file_path, dest_path)

# Optional: clean up temp folder
mssparkutils.fs.rm(source_path, recurse=True)

print("✅ Generated fabric_customer_churn_data.csv.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/customer_churn_data.csv")
# df now is a Spark DataFrame containing CSV data from "Files/customer_churn_data.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
