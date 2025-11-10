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

from notebookutils import mssparkutils
import json
from pyspark.sql import SparkSession
from datetime import datetime

# Access parameters
try:
    validCount = int(mssparkutils.get_parameter("validCount"))
except:
    validCount = 0

try:
    invalidCount = int(mssparkutils.get_parameter("invalidCount"))
except:
    invalidCount = 0

try:
    totalrecords = int(mssparkutils.get_parameter("TotalRowCount"))
except:
    totalrecords = 0

# Print for debugging
print("Pipeline Debug Summary")
print(f"Valid Rows: {validCount}")
print(f"Invalid Rows: {invalidCount}")

# Create Spark session
spark = SparkSession.builder.getOrCreate()

# Create a DataFrame with audit values
audit_df = spark.createDataFrame([{
    "timestamp": datetime.utcnow().isoformat(),
    "validCount": validCount,
    "invalidCount": invalidCount,
    "totalrecords": totalrecords
}])

#if you have not explicitly attached the notebook to the lakehouse
#spark.sql("USE laserengravelakehouse")


# Write to Lakehouse table
audit_df.write.mode("append").saveAsTable("LogCustomerDataValidityCounts")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
