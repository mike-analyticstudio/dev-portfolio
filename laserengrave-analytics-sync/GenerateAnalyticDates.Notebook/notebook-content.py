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
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, year, month, dayofmonth, date_format, weekofyear, quarter, concat, when
from datetime import datetime, timedelta
import requests
from datetime import datetime

# Create Spark session
spark = SparkSession.builder.getOrCreate()

# Generate date range
start_date = datetime(2015, 1, 1)
end_date = datetime(2030, 12, 31)
date_list = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

# Convert to DataFrame
df = spark.createDataFrame([(d,) for d in date_list], ["Date"])

response = requests.get("https://www.gov.uk/bank-holidays.json")
holidays = response.json()["england-and-wales"]["events"]

uk_holiday_dates = [datetime.strptime(h["date"], "%Y-%m-%d").date() for h in holidays]

holiday_df = spark.createDataFrame([(d,) for d in uk_holiday_dates], ["HolidayDate"])


# Add date attributes
df = df.withColumn("Year", year(col("Date"))) \
       .withColumn("Month", month(col("Date"))) \
       .withColumn("MonthName", date_format(col("Date"), "MMMM")) \
       .withColumn("Quarter", quarter(col("Date"))) \
       .withColumn("QuarterLabel", concat(lit("Q"), quarter(col("Date")))) \
       .withColumn("WeekOfYear", weekofyear(col("Date"))) \
       .withColumn("Day", dayofmonth(col("Date"))) \
       .withColumn("DayOfWeek", date_format(col("Date"), "EEEE")) \
       .withColumn("IsWeekend", col("DayOfWeek").isin(["Saturday", "Sunday"]))

df = df.withColumn("FiscalYear", 
    when(month("Date") >= 4, year("Date")).otherwise(year("Date") - 1)
)

df = df.withColumn("FiscalMonth", 
    when(month("Date") >= 4, month("Date") - 3).otherwise(month("Date") + 9)
)

df = df.withColumn("FiscalQuarter", 
    when(month("Date").between(4, 6), "Q1")
    .when(month("Date").between(7, 9), "Q2")
    .when(month("Date").between(10, 12), "Q3")
    .otherwise("Q4")
)

#Join date table to holiday table
df = df.join(holiday_df, df["Date"] == holiday_df["HolidayDate"], "left") \
       .withColumn("IsUKHoliday", when(holiday_df["HolidayDate"].isNotNull(), True).otherwise(False))



# Save to Lakehouse
df.write.mode("overwrite").saveAsTable("Dimdate")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM laserengravelakehouse.dimdate LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
