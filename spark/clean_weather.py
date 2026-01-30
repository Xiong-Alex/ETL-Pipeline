import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# -----------------------------
# CLI arguments (Airflow passes these)
# -----------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Clean weather data")
    parser.add_argument("--raw", required=True, help="Raw input CSV path")
    parser.add_argument("--curated", required=True, help="Curated output path")
    parser.add_argument("--rejected", required=True, help="Rejected output path")
    return parser.parse_args()

# -----------------------------
# Spark job
# -----------------------------
def main():
    args = parse_args()

    spark = SparkSession.builder.appName("clean_weather").getOrCreate()

    # Read raw data
    df = (
        spark.read
        .option("header", "true")
        .csv(args.raw)
    )

    print("📥 Total rows:", df.count())

    # Rejected = any required field is NULL
    rejected = df.filter(
        col("location_id").isNull() |
        col("latitude").isNull() |
        col("longitude").isNull() |
        col("date").isNull() |
        col("temperature_c").isNull() |
        col("precip_mm").isNull() |
        col("wind_kph").isNull() |
        col("condition").isNull()
    )

    # Curated = fully populated rows
    curated = df.dropna()

    print("Curated rows:", curated.count())
    print("Rejected rows:", rejected.count())

    # Write curated data
    curated.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(args.curated)

    # Write rejected data
    rejected.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(args.rejected)

    spark.stop()

# -----------------------------
# Entrypoint
# -----------------------------
if __name__ == "__main__":
    main()
