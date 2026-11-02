from pyspark import pipelines as dp
from pyspark.sql import functions as F

batch_table = "bakehouse_project.p_metadata.batch_control"
rows = spark.sql(f"select batch_id from {batch_table} where batch_status='opened'").collect()
if not rows:
    raise Exception("No opened batch found. Run open_batch first.")
batch_id = rows[0]["batch_id"]

fr_rules={
    "r1": "franchise_id is not null",
    "r2": "name is not null"
}

dp.create_streaming_table("bakehouse_project.slv.s_franchises",expect_all=fr_rules)
@dp.view(name="s_franchises_src")
def s_customers_src():
    return (
        spark.readStream.table("bakehouse_project.brz.b_franchises")
            .withColumnRenamed("franchiseID", "franchise_id")
            .withColumnRenamed("supplierID", "supplier_id")
    )

dp.create_auto_cdc_flow(
    target="bakehouse_project.slv.s_franchises",
    source="s_franchises_src",
    keys=["franchise_id"],
    sequence_by="file_modification_time",
    column_list=["franchise_id", "name", "city", "district", "zipcode",
                 "country", "size", "longitude", "latitude", "supplier_id"
                 , "batch_id", "load_ts", "file_modification_time"],
    stored_as_scd_type="2",
)