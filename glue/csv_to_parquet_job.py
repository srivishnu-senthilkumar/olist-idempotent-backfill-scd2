"""
Glue job: raw Olist CSV tables -> cleaned Parquet in s3://BUCKET/curated/

Reads the raw tables defined in sql/01_raw_tables.sql (database olist_raw),
fixes data types, removes duplicate keys, and writes Parquet. Orders are
partitioned by purchase year and month (about 25 partitions). Partitioning by
day would make about 600 tiny files, which is slower to query, not faster.

Overwrite mode means you can rerun the job and get the same result.
Job parameter: --BUCKET <bucket name, no s3:// prefix>
"""

import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

DB = "olist_raw"

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
BUCKET = args["BUCKET"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Only overwrite the partitions we write, not the whole folder
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


def read_table(name):
    return glueContext.create_dynamic_frame.from_catalog(
        database=DB, table_name=name
    ).toDF()


def write_parquet(df, name, partition_cols=None):
    writer = df.write.mode("overwrite").format("parquet")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(f"s3://{BUCKET}/curated/{name}/")
    print(f"Wrote {name}: {df.count()} rows")


def drop_header_rows(df, key_col):
    # Safety net: remove any row where the key column holds its own column name
    return df.filter(F.col(key_col) != key_col)


# ---- customers ----
customers = drop_header_rows(read_table("customers"), "customer_id").dropDuplicates(["customer_id"])
write_parquet(customers, "customers")

# ---- orders ----
orders = drop_header_rows(read_table("orders"), "order_id").dropDuplicates(["order_id"])
ts_cols = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]
for c in ts_cols:
    orders = orders.withColumn(
        c, F.to_timestamp(F.when(F.trim(F.col(c)) == "", None).otherwise(F.col(c)))
    )

orders = (
    orders
    .withColumn("order_year", F.year("order_purchase_timestamp"))
    .withColumn("order_month", F.month("order_purchase_timestamp"))
)
write_parquet(orders, "orders", ["order_year", "order_month"])

job.commit()
