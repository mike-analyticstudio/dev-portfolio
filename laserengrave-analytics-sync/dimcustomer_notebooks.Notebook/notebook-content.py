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

# <mark>**We want to see the properties of the customer dimension raw file**</mark>

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
# List files in the Lakehouse Files folder
display(spark.read.format("binaryFile").load("Files/dimension_customer.csv"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>List the files located in our lakehouse Delta Files folder</mark>

# CELL ********************

# List files in the Lakehouse Files folder
display(spark.read.format("binaryFile").load("Files"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Quick visual inspection of the customer csv raw data - include the headers - show us the top 5 rows**

# CELL ********************

# Read CSV from Lakehouse Files folder
csv_path = "Files/dimension_customer.csv"
df = spark.read.option("header", True).csv(csv_path)

# Preview the data
df.show(5)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Write csv file now placed in a DataFrame memory to a parquet format of file location provided - so file would be written to a folder <span style="background-color:pink; color:black;">**customer_data_parquet**</span>

# CELL ********************

# Write DataFrame to Parquet format
parquet_path = "Files/customer_data_parquet"
df.write.mode("overwrite").parquet(parquet_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# preview the parquet file created from the folder **customer_data_parquet**

# CELL ********************

# Read the Parquet file
df_parquet = spark.read.parquet(parquet_path)
df_parquet.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Save data to SQL Lakehouse table**

# CELL ********************

# Save as a Lakehouse table
df_parquet.write.mode("overwrite").saveAsTable("dimcustomer")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>**select a few records to show delta table created and contains data**</mark>

# CELL ********************

spark.sql("SELECT * FROM dimcustomer").show(5)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
