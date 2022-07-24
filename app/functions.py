from pyspark.sql import SparkSession, DataFrame


def get_tables(spark: SparkSession) -> DataFrame:
    return spark.sql("show databases")


def create_temporal_delta_directory(dataframe: DataFrame, location: str) -> None:
    dataframe.write.format("delta").mode("overwrite").save(location)


def create_temporal_delta_table(dataframe: DataFrame, table_name: str) -> None:
    dataframe.write.mode("overwrite").saveAsTable(table_name)
