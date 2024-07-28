import pytest
from scd_functions import column_renamer, get_hash
from conftest import spark

def test_column_renamer_append(spark):
    data = [(1, "A"), (2, "B")]
    df = spark.createDataFrame(data, ["id", "name"])

    result_df = column_renamer(df, "_suffix", True)
    
    assert result_df.columns == ["id_suffix", "name_suffix"]

def test_column_renamer_replace(spark):
    data = [(1, "A"), (2, "B")]
    df = spark.createDataFrame(data, ["id_suffix", "name_suffix"])

    result_df = column_renamer(df, "_suffix", False)
    
    assert result_df.columns == ["id", "name"]

def test_get_hash_with_columns(spark):
    data = [(1, "A"), (2, "B")]
    df = spark.createDataFrame(data, ["id", "name"])

    result_df = get_hash(df, ["id", "name"])
    
    assert "hash_md5" in result_df.columns
    assert result_df.select("hash_md5").count() == 2

def test_get_hash_without_columns(spark):
    data = [(1, "A"), (2, "B")]
    df = spark.createDataFrame(data, ["id", "name"])

    result_df = get_hash(df, [])
    
    assert "hash_md5" in result_df.columns
    assert result_df.select("hash_md5").distinct().count() == 1