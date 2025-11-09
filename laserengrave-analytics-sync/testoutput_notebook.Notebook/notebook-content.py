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

'''
df = spark.read.table("lakehouse.customer_data_from_dataflow_invalid")
df.write.mode("append").saveAsTable("lakehouse.customer_data_from_dataflow_invalid_archive")
'''
import json

from notebookutils import mssparkutils

validCount = 123
invalidCount = 45

# Return structured output to pipeline
mssparkutils.notebook.exit(json.dumps({
    "validCount": validCount,
    "invalidCount": invalidCount
}))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
