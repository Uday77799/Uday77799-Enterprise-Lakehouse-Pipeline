# Databricks notebook source
df = spark.read.format("delta") \
    .load("/databricks-datasets/nyctaxi/tables/nyctaxi_yellow")

display(df.limit(10))

# COMMAND ----------

# 1. First, we define 'filtered_df' so the computer remembers it
filtered_df = df.filter((df.passenger_count > 2) & (df.trip_distance > 5) & (df.passenger_count <= 8))

# 2. Import the math function
from pyspark.sql.functions import avg

# 3. Now we can group and aggregate the 'filtered_df' safely
summary_df = filtered_df.groupBy("passenger_count") \
    .agg(avg("fare_amount").alias("average_fare"))

# 4. Display the results
display(summary_df)


# COMMAND ----------

# We are telling Spark to WRITE the data instead of READ it.
# We choose the 'delta' format.
# "overwrite" means if we run this script again tomorrow, it will replace the old data.
summary_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("taxi_fare_summary")

print("Success! The clean data is now permanently saved as a Delta table.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's find out which passenger group is the most profitable
# MAGIC SELECT 
# MAGIC     passenger_count, 
# MAGIC     average_fare 
# MAGIC FROM taxi_fare_summary 
# MAGIC ORDER BY average_fare DESC;

# COMMAND ----------

tables_to_ingest = [
    {"name": "yellow_taxis", "path": "/databricks-datasets/nyctaxi/tables/nyctaxi_yellow"},
    {"name": "green_taxis", "path": "/databricks-datasets/nyctaxi/tables/nyctaxi_green"},
    {"name": "yellow_taxis_backup", "path": "/databricks-datasets/nyctaxi/tables/nyctaxi_yellow"} # Added a 3rd table to prove it keeps going
]

for table in tables_to_ingest:
    print(f"Starting ingestion for: {table['name']}")
    
    try:
        # We TRY to do the risky thing (loading the file)
        temp_df = spark.read.format("delta").load(table['path'])
        row_count = temp_df.count()
        print(f"✅ Success: Loaded {row_count} rows for {table['name']}.")
        
    except Exception as e:
        # If it fails, we catch the error here so the program doesn't crash
        print(f"❌ Warning: Could not load {table['name']}. Skipping to next table.")
        
    print("-" * 30)

# COMMAND ----------

# Render the dataframe directly to the results grid
display(summary_df)

# COMMAND ----------

from pyspark.sql.functions import when, col

# Using your actual Yellow Taxi data
# Let's create the AI feature: High-Value Trip (Over $50)
ai_ready_df = df.withColumn(
    "is_premium_trip", 
    when(col("total_amount") > 50, 1).otherwise(0)
)

# Show the new 'AI-Ready' column alongside the fare
display(ai_ready_df.select("vendor_id", "total_amount", "is_premium_trip"))
# --- SNOWFLAKE LOAD ---

# 1. Define your Snowflake connection properties
sfOptions = {
  "sfUrl": "YDTUQKX-EEC82979.snowflakecomputing.com",
  "sfUser": "UDAY77799",
  "sfPassword": "REDACTED", 
  "sfDatabase": "TAXI_DB",
  "sfSchema": "PUBLIC",
  "sfWarehouse": "COMPUTE_WH"
}

# 2. Write the Gold DataFrame directly into Snowflake
ai_ready_df.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "GOLD_TAXI_PREMIUM_TRIPS") \
  .mode("overwrite") \
  .save()

print("Success! Gold data successfully loaded into Snowflake Data Warehouse.")
