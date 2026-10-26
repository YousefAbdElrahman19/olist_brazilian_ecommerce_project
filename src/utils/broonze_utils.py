from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def create_bronze_table(table_name, path, file_format="csv", pattern=None):
    """Define one bronze table that reads raw files with Auto Loader."""

    @dp.table(
        name=f"olist_brazilian_ecommerce.bronze.{table_name}_raw",
        comment=f"Raw {table_name} loaded from {path}",
    )
    def _bronze():
        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", file_format)
        )

        if pattern:
            reader = reader.option("pathGlobFilter", pattern)

        if file_format == "csv":
            reader = (
                reader.option("header", "true")
                .option("cloudFiles.inferColumnTypes", "false")  # keep all as string
                .option("multiLine", "true")
                .option("quote", '"')
                .option("escape", '"')
            )

        return (
            reader.load(path)
            .withColumn("ingest_time", F.current_timestamp())
            .withColumn("source_file", F.col("_metadata.file_path"))
        )