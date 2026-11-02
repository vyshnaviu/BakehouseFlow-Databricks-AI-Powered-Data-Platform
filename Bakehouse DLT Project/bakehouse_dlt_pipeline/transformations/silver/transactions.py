from pyspark import pipelines as dp
from pyspark.sql.functions import *

### Streaming Table
batch_table="bakehouse_project.p_metadata.batch_control"
batch_id=spark.sql(f"""select batch_id from {batch_table} where batch_status='opened' """).collect()[0]['batch_id']


tra_rules={
    "r1": "transaction_id is not null",
    "r2": "customer_id is not null",
    "r3": "franchise_id is not null"

}

@dp.table(name='bakehouse_project.slv.s_transactions')
def s_supplier_src():
    df=spark.readStream.table('bakehouse_project.brz.b_transactions')
    df=df.withColumnRenamed('transactionID','transaction_id')\
        .withColumnRenamed('franchiseID','franchise_id')\
            .withColumnRenamed('customerID','customer_id')\
                .withColumnRenamed('dateTime','paid_time')\
                    .withColumnRenamed('unitPrice','unit_price')\
                        .withColumnRenamed('totalPrice','total_price')\
                            .withColumnRenamed('cardNumber','card_number')\
                                .withColumnRenamed('paymentMethod','payment_method')
    df=df.select('transaction_id','franchise_id','customer_id','paid_time','product','quantity','unit_price','total_price','card_number','payment_method','load_ts','batch_id')
    return df

