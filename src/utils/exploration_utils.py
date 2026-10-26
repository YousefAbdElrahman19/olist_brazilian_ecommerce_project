from pyspark.sql import DataFrame, functions as F

FAKE_NULLS = ["", "NA", "N/A", "null", "NULL", "none", "None"]

def profile_nulls(df: DataFrame) -> DataFrame:
    """Null count and % per column, in one Spark job.
    Strings also count as null if empty or one of FAKE_NULLS."""
    aggs = [F.count(F.lit(1)).alias("__total__")]
    for name, dtype in df.dtypes:
        bad = F.col(name).isNull()
        if dtype == "string":
            bad = bad | F.trim(F.col(name)).isin(*FAKE_NULLS)
        aggs.append(F.sum(F.when(bad, 1).otherwise(0)).alias(name))

    row = df.agg(*aggs).first().asDict()
    total = row.pop("__total__")

    rows = [
        (c, int(n or 0), round((n or 0) / total * 100, 2) if total else 0.0)
        for c, n in row.items()
    ]
    return df.sparkSession.createDataFrame(
        rows, ["column_name", "null_count", "null_percentage"]
    ).orderBy(F.desc("null_count"))


def check_key_uniqueness(df: DataFrame, key_cols: list) -> dict:
    """Is the supposed key really unique? Are any key values null?"""
    total = df.count()
    distinct = df.select(*key_cols).distinct().count()
    null_keys = df.filter(
        " OR ".join(f"`{c}` IS NULL" for c in key_cols)
    ).count()
    return {
        "rows": total,
        "distinct_keys": distinct,
        "duplicate_rows": total - distinct,
        "null_keys": null_keys,
    }




def check_duplicates(
    df: DataFrame,
    columns: list[str] | None = None
) -> None:
    # Use all columns if no column list is provided
    cols = columns if columns is not None else df.columns

    # Validate column names
    invalid_cols = [col for col in cols if col not in df.columns]
    if invalid_cols:
        raise ValueError(f"Columns not found: {invalid_cols}")

    total_rows = df.count()

    distinct_rows = df.select(*cols).distinct().count()

    duplicate_count = total_rows - distinct_rows

    print(f"Total rows: {total_rows}")
    print(f"Distinct combinations: {distinct_rows}")
    print(f"Duplicate rows: {duplicate_count}")