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

# CELL ********************

from notebookutils import mssparkutils
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# Parameters
env = mssparkutils.get_parameter("EnvironmentFlag")
validCount = int(mssparkutils.get_parameter("validCount"))
invalidCount = int(mssparkutils.get_parameter("invalidCount"))

# Create audit record
audit_df = spark.createDataFrame([{
    "timestamp": datetime.utcnow().isoformat(),
    "EnvironmentFlag": env,
    "validCount": validCount,
    "invalidCount": invalidCount,
    "ValidationStatus": "Success" if invalidCount == 0 else "Failed"
}])

# Write to Lakehouse
audit_df.write.mode("append").saveAsTable("Audit_ValidationRuns")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
