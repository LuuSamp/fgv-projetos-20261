"""Load: S3 merge, Glue Catalog partitions, watermark update."""

from __future__ import annotations

from datetime import datetime, timezone

import boto3
from constants import FACT_DATA_COLUMNS, FACT_KEYS, watermark_date_str
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def _path_exists(spark: SparkSession, path: str) -> bool:
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(path),
        spark._jsc.hadoopConfiguration(),
    )
    return fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path))


def _exec_jdbc_update(*, spark: SparkSession, url: str, user: str, password: str, sql: str) -> None:
    jvm = spark._jvm
    conn = jvm.java.sql.DriverManager.getConnection(url, user, password)
    stmt = conn.createStatement()
    try:
        stmt.executeUpdate(sql)
    finally:
        stmt.close()
        conn.close()


def merge_into_prefix(
    *,
    spark: SparkSession,
    delta_df: DataFrame,
    base_path: str,
    merge_keys: list[str],
    data_columns: list[str],
) -> bool:
    if delta_df.rdd.isEmpty():
        return False

    delta = delta_df.select(*(merge_keys + data_columns)).dropDuplicates(merge_keys)
    if _path_exists(spark, base_path):
        existing = spark.read.parquet(base_path)
        for col in data_columns:
            if col not in existing.columns:
                existing = existing.withColumn(col, F.lit(None))
        existing = existing.select(*(merge_keys + data_columns))
        merged = (
            existing.join(delta.select(*merge_keys), on=merge_keys, how="left_anti")
            .select(*(merge_keys + data_columns))
            .unionByName(delta)
        )
    else:
        merged = delta

    merged.coalesce(1).write.mode("overwrite").parquet(base_path)
    return True


def merge_fact_partitions(*, spark: SparkSession, delta_fact: DataFrame, fact_base_path: str) -> list[tuple[int, int]]:
    if delta_fact.rdd.isEmpty():
        return []

    partitions = delta_fact.select("order_year", "order_month").distinct().collect()
    touched: list[tuple[int, int]] = []
    for part in partitions:
        year = int(part["order_year"])
        month = int(part["order_month"])
        part_path = f"{fact_base_path}/order_year={year}/order_month={month}/"
        part_delta = delta_fact.filter(
            (F.col("order_year") == year) & (F.col("order_month") == month)
        )
        merge_into_prefix(
            spark=spark,
            delta_df=part_delta,
            base_path=part_path,
            merge_keys=FACT_KEYS,
            data_columns=FACT_DATA_COLUMNS,
        )
        touched.append((year, month))
    return touched


def register_fact_partitions(*, glue_database: str, fact_base_path: str, partitions: list[tuple[int, int]]) -> None:
    if not partitions:
        return

    glue = boto3.client("glue")
    entries = []
    for year, month in partitions:
        location = f"{fact_base_path}/order_year={year}/order_month={month}/"
        entries.append(
            {
                "Values": [str(year), str(month)],
                "StorageDescriptor": {
                    "Location": location,
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    },
                },
            }
        )
    glue.batch_create_partition(
        DatabaseName=glue_database,
        TableName="fact_orders",
        PartitionInputList=entries,
    )


def update_watermark(
    *,
    spark: SparkSession,
    jdbc_url: str,
    user: str,
    password: str,
    pipeline_name: str,
    watermark_table: str,
    status: str,
    last_processed_order_date=None,
) -> None:
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    if last_processed_order_date is not None:
        date_str = watermark_date_str(last_processed_order_date)
        sql = (
            f"UPDATE {watermark_table} SET "
            f"last_processed_order_date = DATE('{date_str}'), "
            f"last_run_at = '{now_utc}', "
            f"last_run_status = '{status}' "
            f"WHERE pipeline_name = '{pipeline_name}'"
        )
    else:
        sql = (
            f"UPDATE {watermark_table} SET "
            f"last_run_at = '{now_utc}', "
            f"last_run_status = '{status}' "
            f"WHERE pipeline_name = '{pipeline_name}'"
        )
    _exec_jdbc_update(spark=spark, url=jdbc_url, user=user, password=password, sql=sql)
