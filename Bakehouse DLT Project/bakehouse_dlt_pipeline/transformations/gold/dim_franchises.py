from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window

SLV = "bakehouse_project.slv"
GLD = "bakehouse_project.gld"


# ---------- helper ----------
def scd2_prep(df, id_col):
    """SCD2 versions: audit columns plus start_at / end_at for point-in-time joins."""
    w = Window.partitionBy(id_col).orderBy("__START_AT")
    return (
        df.withColumn("insert_batch_id", F.col("batch_id"))
          .withColumn("update_batch_id", F.coalesce(F.lead("batch_id").over(w), F.col("batch_id")))
          .withColumn("insert_date", F.col("__START_AT"))
          .withColumn("update_date", F.coalesce(F.col("__END_AT"), F.col("__START_AT")))
          .withColumn("start_at",
                      F.when(F.row_number().over(w) == 1, F.lit("1900-01-01").cast("timestamp"))
                       .otherwise(F.col("__START_AT")))
          .withColumnRenamed("__END_AT", "end_at")
          .drop("__START_AT", "batch_id")
    )


# ---------- dim_franchise (SCD2, linked to supplier version) ----------
@dp.materialized_view(name=f"{GLD}.dim_franchise")
@dp.expect_or_drop("valid_franchise_id", "franchise_id IS NOT NULL")
def dim_franchise():
    f = (scd2_prep(spark.read.table(f"{SLV}.s_franchises"), "franchise_id")
            .withColumnRenamed("supplierID", "supplier_id")
            .alias("f"))
    s = (spark.read.table(f"{GLD}.dim_supplier")
              .select("supplier_id", "supplier_key",
                      F.col("start_at").alias("s_start"), F.col("end_at").alias("s_end"))
              .alias("s"))
    joined = f.join(
        s,
        (F.col("f.supplier_id") == F.col("s.supplier_id"))
        & (F.col("f.start_at") >= F.col("s.s_start"))
        & (F.col("s.s_end").isNull() | (F.col("f.start_at") < F.col("s.s_end"))),
        "left",
    )
    return (
        joined.select("f.*", "s.supplier_key")
              .withColumn("franchise_key", F.xxhash64("franchise_id", "start_at"))
    )
