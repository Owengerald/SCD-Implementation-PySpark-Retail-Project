from pyspark.sql import DataFrame
from pyspark.sql.functions import col, md5, concat_ws, lit

def column_renamer(df: DataFrame, suffix: str, append: bool) -> DataFrame:
    if append:
        new_column_names = list(map(lambda x: x+suffix, df.columns))
    else:
        new_column_names = list(map(lambda x: x.replace(suffix, ""), df.columns))
    return df.toDF(*new_column_names)

def get_hash(df: DataFrame, keys_list: list) -> DataFrame:
    columns = [col(column) for column in keys_list]
    if columns:
        return df.withColumn("hash_md5", md5(concat_ws("", *columns)))
    else:
        return df.withColumn("hash_md5", md5(lit("1")))