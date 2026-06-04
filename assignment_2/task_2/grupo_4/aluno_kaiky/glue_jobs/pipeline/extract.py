"""JDBC extraction: watermark read and incremental delta from classicmodels."""

from __future__ import annotations
from dataclasses import dataclass
from pyspark.sql import DataFrame, SparkSession

_MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"


def _read_query(*, spark: SparkSession, url: str, user: str, password: str, query: str) -> DataFrame:
    wrapped = f"({query}) AS incremental_src"
    return (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", wrapped)
        .option("user", user)
        .option("password", password)
        .option("driver", _MYSQL_DRIVER)
        .load()
    )


def _read_table(*, spark: SparkSession, url: str, user: str, password: str, table: str) -> DataFrame:
    return _read_query(spark=spark, url=url, user=user, password=password, query=f"SELECT * FROM {table}")


def read_watermark(
    *,
    spark: SparkSession,
    jdbc_url: str,
    user: str,
    password: str,
    pipeline_name: str,
    watermark_table: str,
) -> dict:
    sql = (
        f"SELECT pipeline_name, last_processed_order_date, last_run_at, last_run_status "
        f"FROM {watermark_table} WHERE pipeline_name = '{pipeline_name}'"
    )
    rows = _read_query(spark=spark, url=jdbc_url, user=user, password=password, query=sql).collect()
    if not rows:
        raise ValueError(f"Watermark row missing for pipeline_name={pipeline_name}")
    row = rows[0]
    return {
        "pipeline_name": row["pipeline_name"],
        "last_processed_order_date": row["last_processed_order_date"],
        "last_run_at": row["last_run_at"],
        "last_run_status": row["last_run_status"],
    }


@dataclass
class DeltaExtract:
    orders: DataFrame
    orderdetails: DataFrame
    customers: DataFrame
    products: DataFrame
    productlines: DataFrame
    employees: DataFrame
    offices: DataFrame


def extract_delta(
    *,
    spark: SparkSession,
    jdbc_url: str,
    user: str,
    password: str,
    cutoff: str,
) -> DeltaExtract | None:
    """Extract order delta and dimension sources. Returns None when there are no new orders."""
    orders = _read_query(
        spark=spark,
        url=jdbc_url,
        user=user,
        password=password,
        query=f"SELECT * FROM orders WHERE orderDate > DATE('{cutoff}')",
    )
    order_numbers = [int(r["orderNumber"]) for r in orders.select("orderNumber").distinct().collect()]
    if not order_numbers:
        return None

    in_clause = ",".join(str(n) for n in order_numbers)
    orderdetails = _read_query(
        spark=spark,
        url=jdbc_url,
        user=user,
        password=password,
        query=f"SELECT * FROM orderdetails WHERE orderNumber IN ({in_clause})",
    )
    customers = _read_table(spark=spark, url=jdbc_url, user=user, password=password, table="customers")
    products = _read_table(spark=spark, url=jdbc_url, user=user, password=password, table="products")
    productlines = _read_table(spark=spark, url=jdbc_url, user=user, password=password, table="productlines")
    employees = _read_table(spark=spark, url=jdbc_url, user=user, password=password, table="employees")
    offices = _read_table(spark=spark, url=jdbc_url, user=user, password=password, table="offices")

    return DeltaExtract(
        orders=orders,
        orderdetails=orderdetails,
        customers=customers,
        products=products,
        productlines=productlines,
        employees=employees,
        offices=offices,
    )
