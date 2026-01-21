from pyspark.sql import SparkSession

from config import schema


def get_bronze_layer(spark: SparkSession):
    cust_info_df = spark.read.csv(
        "./data/lake/customer_info.csv",
        header=True,
        schema=schema.customer_info_bronze_schema,
    )
    product_info_df = spark.read.csv(
        "./data/lake/product_info.csv",
        header=True,
        schema=schema.product_info_bronze_schema,
    )
    sales_info_df = spark.read.csv(
        "./data/lake/sales_info.csv",
        header=True,
        schema=schema.sales_info_bronze_schema,
    )

    return [cust_info_df, product_info_df, sales_info_df]
