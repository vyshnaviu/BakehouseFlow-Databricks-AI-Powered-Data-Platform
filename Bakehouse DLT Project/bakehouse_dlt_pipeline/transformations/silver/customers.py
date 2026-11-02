from pyspark import pipelines as dp
from pyspark.sql import functions as F

batch_table = "bakehouse_project.p_metadata.batch_control"
rows = spark.sql(f"select batch_id from {batch_table} where batch_status='opened'").collect()
if not rows:
    raise Exception("No opened batch found. Run open_batch first.")
batch_id = rows[0]["batch_id"]

cu_rules={
    "r1": "customer_id is not null"
}

dp.create_streaming_table("bakehouse_project.slv.s_customers",expect_all=cu_rules)

@dp.view(name="s_customers_src")
def s_customers_src():
    return (
        spark.readStream.table("bakehouse_project.brz.b_customers")
            .withColumn("name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
            .withColumnRenamed("customerID", "customer_id")
    )

dp.create_auto_cdc_flow(
    target="bakehouse_project.slv.s_customers",
    source="s_customers_src",
    keys=["customer_id"],
    sequence_by="file_modification_time",
    column_list=["customer_id", "name", "email_address", "phone_number", "address",
                 "city", "state", "country", "continent", "postal_zip_code",
                 "gender", "batch_id", "load_ts", "file_modification_time"],
    stored_as_scd_type="1",
)