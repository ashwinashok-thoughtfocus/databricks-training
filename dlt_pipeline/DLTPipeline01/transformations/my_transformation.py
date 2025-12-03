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
        .load("/Volumes/quickstart_catalog/quickstart_schema/sandbox/dataset/e-commerce/staging/orders/")
    )

@dlt.table(
    name="silver_orders",
    comment="Derive KPI's",
    table_properties={"quality": "silver"}
)
# @dlt.expect_or_drop("Valid Order Id", "order_id IS NOT NULL")
@dlt.expect("qty", "qty > 0")
def load_orders_to_silver():
    df = dlt.read("bronze_orders")
    return df.withColumn("total_price", col("price") * col("qty"))

@dlt.table(
    name="gold_orders",
    table_properties={"quality": "gold"}
)
def load_orders_to_gold():
    df = dlt.read("silver_orders")
    return df.groupBy("customer_id").count()