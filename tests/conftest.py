import uuid

from pyspark.sql import SparkSession

import pytest


@pytest.fixture()
def spark_session():
    return (
        SparkSession.builder.appName("PySpark Example with Delta")
        .config("spark.sql.warehouse.dir", f"/tmp/test_spark_{str(uuid.uuid4())}")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .master("local[*]")
        .getOrCreate()
    )
