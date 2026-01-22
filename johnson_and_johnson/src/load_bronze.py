import os

from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("load_bronze").getOrCreate()

    input_files = [
        "maintenance_events.csv",
        "manufacturing_factory_dataset.csv",
        "operators_roster.csv",
    ]

    bronze_base = "johnson_and_johnson/data/bronze"
    parquet_base = os.path.join(bronze_base, "parquet")
    csv_base = os.path.join(bronze_base, "csv")
    os.makedirs(parquet_base, exist_ok=True)
    os.makedirs(csv_base, exist_ok=True)

    for filename in input_files:
        dataset_name = os.path.splitext(filename)[0]
        input_path = os.path.join("input_data", dataset_name, filename)
        df = spark.read.option("header", "true").csv(input_path)
        df.write.mode("overwrite").parquet(os.path.join(parquet_base, dataset_name))
        df.write.mode("overwrite").option("header", "true").csv(
            os.path.join(csv_base, dataset_name)
        )

    spark.stop()


if __name__ == "__main__":
    main()
