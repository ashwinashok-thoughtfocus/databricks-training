import dlt
from pyspark.sql.functions import col

@dlt.table(
    name="bronze_orders",
    comment="Raw orders ingested from staging to bronze_orders table",
)
def load_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/Volumes/quickstart_catalog/quickstart_schema/sandbox/dataset/e-commerce_new/staging/orders/")
    )

@dlt.table(
    name="bronze_customers",
    comment="Raw customers ingested from staging to bronze_customers table",
)
def load_customers():
    return (
        spark.read.format("json")
        .load("/Volumes/quickstart_catalog/quickstart_schema/sandbox/dataset/e-commerce_new/staging/customers/")
    )

@dlt.table(
    name="silver_customers_orders",
    comment="Derive KPI's",
    table_properties={"quality": "silver"}
)
def load_customers_orders_to_silver():
   orders_df = dlt.read("bronze_orders")
   customers_df = dlt.read("bronze_customers")
   result_df  = customers_df.join(orders_df , on="customer_id", how='inner')
   return result_df

@dlt.table(
    name="gold_customers_orders",
    table_properties={"quality": "gold"}
)
def load_orders_to_gold():
    df = dlt.read("silver_customers_orders")
    return df.groupBy("name").count()