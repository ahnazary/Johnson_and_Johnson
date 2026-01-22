import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def read_bronze_dataset(spark: SparkSession, bronze_base: str, dataset: str):
    csv_path = os.path.join(bronze_base, "csv", dataset)
    parquet_path = os.path.join(bronze_base, "parquet", dataset)

    df_csv = spark.read.option("header", "true").csv(csv_path)
    df_parquet = spark.read.parquet(parquet_path)

    columns = df_parquet.columns or df_csv.columns
    df_csv = _align_columns_as_string(df_csv, columns)
    df_parquet = _align_columns_as_string(df_parquet, columns)

    return df_parquet.unionByName(df_csv)


def _align_columns_as_string(df, columns):
    """
    Align DataFrame columns to the specified list of columns, casting all to string type.
    If a column from the list does not exist in the DataFrame, it will be added with null values.
    """
    return df.select(
        [
            F.col(col).cast("string") if col in df.columns else F.lit(None).cast("string")
            for col in columns
        ]
    )


def main() -> None:
    spark = SparkSession.builder.appName("load_silver").getOrCreate()

    bronze_base = "johnson_and_johnson/data/bronze"
    silver_base = "johnson_and_johnson/data/silver"
    os.makedirs(silver_base, exist_ok=True)

    maintenance = read_bronze_dataset(spark, bronze_base, "maintenance_events").dropDuplicates(
        ["event_id"]
    )
    maintenance.write.mode("overwrite").parquet(
        os.path.join(silver_base, "maintenance_events")
    )

    factory = read_bronze_dataset(
        spark, bronze_base, "manufacturing_factory_dataset"
    ).dropDuplicates(["order_id"])
    factory.write.mode("overwrite").parquet(
        os.path.join(silver_base, "manufacturing_factory_dataset")
    )

    operators = read_bronze_dataset(spark, bronze_base, "operators_roster").dropDuplicates(
        ["operator_id"]
    )
    operators.write.mode("overwrite").parquet(
        os.path.join(silver_base, "operators_roster")
    )

    spark.stop()


if __name__ == "__main__":
    main()
