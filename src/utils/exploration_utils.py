from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, desc

def count_nulls(df, spark):
    total_rows = df.count()

    null_counts = df.select([
        sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ]).first().asDict()

    rows = [
        (
            column,
            int(null_counts[column] or 0),
            round((null_counts[column] or 0) / total_rows * 100, 2)
            if total_rows else 0.0
        )
        for column in df.columns
    ]

    return spark.createDataFrame(
        rows,
        ["column_name", "null_count", "null_percentage"]
    ).orderBy(desc("null_count"))