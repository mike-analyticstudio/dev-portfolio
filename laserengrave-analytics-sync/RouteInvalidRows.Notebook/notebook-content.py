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

# PARAMETERS CELL ********************

RouteInvalidTo = "Files/test2"
EnvironmentFlag = "PROD"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col, to_date, when, lit, regexp_replace, to_timestamp, year, date_format, quarter, dayofmonth, concat, weekofyear, month

spark = SparkSession.builder.getOrCreate()

# Parameters


#routeInvalidTo = mssparkutils.get_parameter("RouteInvalidTo")
env = EnvironmentFlag #mssparkutils.get_parameter("EnvironmentFlag")


# Read invalid rows
invalid_df = spark.read.table("customer_data_from_dataflow_invalid")

#invalid_df = df.filter("ValidationStatus = 'Failed'") #we have a separate table that holds invalid records

# Add diagnostics
timestamp = datetime.utcnow().isoformat()
partition = datetime.utcnow().strftime("%Y/%m/%d/%H%M")

invalid_df = invalid_df \
                .withColumn("ErrorSource", lit("ValidationNotebook"))\
                .withColumn("ValidationStatus", lit("Failed")) \
                .withColumn("ErrorTags", lit("Missing ID, Bad Date")) \
                .withColumn("EnvironmentFlag", lit(env)) \
                .withColumn("ArchiveTime", lit(timestamp))


# Write to Lakehouse
invalid_df.write.mode("overwrite").parquet(f"{RouteInvalidTo}/partition={partition}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
