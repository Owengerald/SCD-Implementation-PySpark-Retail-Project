import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[2]").appName("pytest").getOrCreate()
    yield spark
    spark.stop()