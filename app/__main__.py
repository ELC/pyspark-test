import uuid

from . import get_tables, create_temporal_delta_directory, create_temporal_delta_table

from pyspark.sql import SparkSession


def main():
    print("Set temporary location for Data")
    warehouse_location = f"/tmp/test_spark_{str(uuid.uuid4())}"
    print(f"Temporary location ready - Data will be saved at: {warehouse_location}")

    print("Testing Spark")
    spark = (
        SparkSession.builder.appName("PySpark Example with Delta")
        .config("spark.sql.warehouse.dir", warehouse_location)
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .master("local[*]")
        .getOrCreate()
    )
    print("=" * 80)
    print("Spark Session successfully created")
    
    print("Testing Tables")
    tables = get_tables(spark)
    table_names_rows = tables.collect()
    table_names = ",".join(row.namespace for row in table_names_rows)
    print(f"Available Tables: {table_names}")
    print("Tables successfully read")

    print("Testing DataFrames")
    dataframe = spark.range(0, 5)
    print("Dataframe successfully created")

    print("Testing Delta - File Format")
    create_temporal_delta_directory(dataframe, "/tmp/test-delta")
    print("Delta successfully saved as parquet")

    print("Testing Delta - Table Format")
    create_temporal_delta_table(dataframe, "test_delta_tables")
    print("Delta successfully saved as table")


if __name__ == "__main__":
    main()
