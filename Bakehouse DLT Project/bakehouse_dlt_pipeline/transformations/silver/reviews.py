from pyspark import pipelines as dp
from pyspark.sql import functions as F

batch_table = "bakehouse_project.p_metadata.batch_control"
rows = spark.sql(f"select batch_id from {batch_table} where batch_status='opened'").collect()
if not rows:
    raise Exception("No opened batch found. Run open_batch first.")
batch_id = rows[0]["batch_id"]

### Expectations
re_rules={
    "r1": "franchise_id is not null",
    "r2": "review_date is not null"
}

dp.create_streaming_table("bakehouse_project.slv.s_reviews",expect_all=re_rules)

@dp.append_flow(target='bakehouse_project.slv.s_reviews')
def customer_reviews():
    df=spark.readStream.table("bakehouse_project.brz.b_media_customer_reviews")
    df=df.withColumnRenamed('franchiseID','franchise_id')
    df = df.withColumn("franchise_id", F.col("franchise_id").cast("int"))
    df = df.withColumn(
        "review_date",
        F.to_date(F.col("review_date"), "yyyy-MM-dd")
    )
    df = df.withColumn("brz_table", F.lit("b_media_customer_reviews"))          
    df=df.select('franchise_id','review_date','review','brz_table','load_ts','batch_id')
    return df

@dp.append_flow(target='bakehouse_project.slv.s_reviews')
def media_reviews():
    df=spark.readStream.table("bakehouse_project.brz.b_media_gold_reviews_chunked")\
        .filter(F.col("batch_id") == batch_id)
    df=df.withColumnRenamed('franchiseID','franchise_id').withColumnRenamed('chunked_text','review')
    df = df.withColumn(
    "review_date",
    F.to_date(F.col("review_date"), "yyyy-MM-dd")
)
    df = df.withColumn("brz_table", F.lit("b_media_gold_reviews_chunked"))          
    df=df.select('franchise_id','review_date','review','brz_table','load_ts','batch_id')
    return df