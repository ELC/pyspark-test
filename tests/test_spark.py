from pyspark.sql import SparkSession

from app import get_tables


def test_spark(spark_session: SparkSession):
    tables = get_tables(spark_session)
    assert tables.count() == 1

    table_names = tables.collect()
    assert table_names[0].namespace == "default"
