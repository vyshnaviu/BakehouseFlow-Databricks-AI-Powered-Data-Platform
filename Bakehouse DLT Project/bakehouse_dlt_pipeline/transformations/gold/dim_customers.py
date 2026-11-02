from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window

SLV = "bakehouse_project.slv"
GLD = "bakehouse_project.gld"
# ---------- dim_customer (SCD1) ----------
@dp.materialized_view(name=f"{GLD}.dim_customer")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def dim_customer():
    return (
        spark.read.table(f"{SLV}.s_customers")
             .withColumn("customer_key", F.xxhash64("customer_id"))
    )
