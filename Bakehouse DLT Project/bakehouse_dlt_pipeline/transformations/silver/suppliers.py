from pyspark import pipelines as dp
from pyspark.sql.functions import *

### Streaming Table
batch_table="bakehouse_project.p_metadata.batch_control"
batch_id=spark.sql(f"""select batch_id from {batch_table} where batch_status='opened' """).collect()[0]['batch_id']

sup_rules={
    "r1": "supplier_id is not null",
    "r2": "name is not null"
}
dp.create_streaming_table("bakehouse_project.slv.s_suppliers",expect_all=sup_rules)

@dp.view()
def s_supplier_src():
    df=spark.readStream.table('bakehouse_project.brz.b_suppliers')
    df=df.withColumnRenamed('supplierID','supplier_id')                                                        
    return df


dp.create_auto_cdc_flow(
  target = "bakehouse_project.slv.s_suppliers",
  source = "s_supplier_src",
  keys = ["supplier_id"],
  sequence_by = "file_modification_time",
  column_list = ["supplier_id","name","ingredient","continent","city","district","size",
"longitude","latitude","approved","batch_id","file_modification_time"], 
  stored_as_scd_type = "2", 
)
