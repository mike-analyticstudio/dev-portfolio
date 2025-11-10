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

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **<mark>We return the rerecord count of the valid table, and the invalid table</mark>**

# CELL ********************

from notebookutils import mssparkutils
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json

spark = SparkSession.builder.getOrCreate()


# Read validated table
df = spark.read.table("laserengravelakehouse.Fact_Inventory_Process")

# Count valid and invalid rows - if in same table using our validation flag

#chained filter i.e. have multiple filter so you can support multiple logic
'''
filtered_df = df.filter(
    (col("sk_valid") == True) &
    (col("Region") == "North") &
    (col("StockLevel") > 100)
)
'''

#filter for valid and invalid records
df_valid = df.filter(col("sk_valid") == True)
df_invalid = df.filter(col("sk_valid") == False)


validCount = df_valid.count()
invalidCount = df_invalid.count()

total_records = validCount + invalidCount

# Return counts to pipeline
mssparkutils.notebook.exit(json.dumps({
    "validCount": validCount,
    "invalidCount": invalidCount,
    "totalrecords": total_records
}))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
