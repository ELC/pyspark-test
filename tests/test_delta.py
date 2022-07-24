from pyspark.sql import SparkSession

import pytest

from app import create_temporal_delta_directory, create_temporal_delta_table


@pytest.fixture
def delta_name():
    return f"test_delta_table"


def test_spark_delta_directory(spark_session: SparkSession, delta_name: str):
    dataframe = spark_session.range(0, 5)
    location = f"/tmp/{delta_name}"

    create_temporal_delta_directory(dataframe, location)
    df = spark_session.read.format("delta").load(location)
    assert df.count() == 5

    last_row, *_ = df.select("id").sort("id", ascending=False).limit(1).collect()
    assert last_row.id == 4


def test_spark_delta_table(spark_session: SparkSession, delta_name: str):
    dataframe = spark_session.range(0, 5)

    create_temporal_delta_table(dataframe, delta_name)
    df = spark_session.read.table(delta_name)
    assert df.count() == 5

    last_row, *_ = df.select("id").sort("id", ascending=False).limit(1).collect()
    assert last_row.id == 4
