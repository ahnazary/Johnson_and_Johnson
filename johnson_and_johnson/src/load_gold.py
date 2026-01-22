import os

from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("load_gold").getOrCreate()

    silver_base = "johnson_and_johnson/data/silver"
    gold_base = "johnson_and_johnson/data/gold"
    os.makedirs(os.path.join(gold_base, "parquet"), exist_ok=True)

    # create dim_operators table parquet
    operators = spark.read.parquet(
        os.path.join(silver_base, "operators_roster")
    ).select("operator_id", "name").dropDuplicates(["operator_id"])

    # write dim_operators table in parquet format
    operators.write.mode("overwrite").parquet(
        os.path.join(gold_base, "parquet/dim_operators")
    )

    # write dim_operators in csv format
    operators.write.mode("overwrite").option("header", "true").csv(
        os.path.join(gold_base, "csv/dim_operators")
    )


    # join manufacturing_factory_dataset events with operators to create manufacturing_factory_dataset table
    manufacturing_factory_dataset = spark.read.parquet(
        os.path.join(silver_base, "manufacturing_factory_dataset")
    )
    manufacturing_factory_dataset = manufacturing_factory_dataset.join(
        operators, on="operator_id", how="left"
    ).select(
        "timestamp",
        "factory_id",
        "line_id",
        "shift",
        "product_id",
        "order_id",
        "planned_qty",
        "produced_qty",
        "scrap_qty",
        "defects_count",
        "defect_type",
        "cycle_time_s",
        "oee",
        "availability",
        "performance",
        "quality",
        "machine_state",
        "downtime_reason",
        "maintenance_type",
        "maintenance_due_date",
        "vibration_mm_s",
        "temperature_c",
        "pressure_bar",
        "energy_kwh",
        "operator_id",
        "name",
        "workorder_status",
    )

    # write manufacturing_factory_dataset table in parquet format
    manufacturing_factory_dataset.write.mode("overwrite").parquet(
        os.path.join(gold_base, "parquet/fct_manufacturing_factory_dataset")
    )

    # write manufacturing_factory_dataset in csv format
    manufacturing_factory_dataset.write.mode("overwrite").option("header", "true").csv(
        os.path.join(gold_base, "csv/fct_manufacturing_factory_dataset")
    )

    spark.stop()

if __name__ == "__main__":
    main()
