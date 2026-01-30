# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col

# spark = SparkSession.builder.appName("clean_weather_minimal").getOrCreate()

# # Hard Coding Paths
# RAW_PATH = "/opt/data/raw/weather_mock1.csv"
# CURATED_PATH = "/opt/data/curated/weather_daily"
# REJECTED_PATH = "/opt/data/rejected/weather_daily"

# # Access Raw Data CSV
# df = (
#     spark.read
#     .option("header", "true")
#     .csv(RAW_PATH)
# )

# print("Total rows: ", df.count())

# # Filter out Rejected Rows
# rejected = df.filter(
#     col("location_id").isNull() |
#     col("latitude").isNull() |
#     col("longitude").isNull() |
#     col("date").isNull() |
#     col("temperature_c").isNull() |
#     col("precip_mm").isNull() |
#     col("wind_kph").isNull() |
#     col("condition").isNull()
# )

# # Rows that are fully populated → curated
# curated = df.dropna()

# ##### Rewrite later to determine Rejected & Curated In one pass

# print("Curated rows:", curated.count())
# print("Rejected rows:", rejected.count())

# # Write curated data
# curated.coalesce(1).write.mode("overwrite") \
#     .option("header", "true") \
#     .csv(CURATED_PATH)

# # Write rejected data
# rejected.coalesce(1).write.mode("overwrite") \
#     .option("header", "true") \
#     .csv(REJECTED_PATH)

# spark.stop()







# # Figure out how to integrate / give access/control to airflow (pass params)
# # hook up PosgreSQL to hold final curated data...
# # 








# #This ETL is experimental
# # To Learn how to work airflow, pySpark, and PosgreSQL
