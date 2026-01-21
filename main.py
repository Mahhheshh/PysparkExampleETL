from pyspark.sql import SparkSession

from pipeline.ingest import get_bronze_layer
from pipeline.transform import (
    transform_customer_info,
    transform_product_info,
    transform_sales_info,
)


def run_pipeline(spark: SparkSession) -> None:
    (customer_bronze_df, product_bronze_df, sales_bronze_df) = get_bronze_layer(spark)

    customer_silver_df = transform_customer_info(customer_bronze_df)
    product_silver_df = transform_product_info(product_bronze_df)
    sales_silver_df = transform_sales_info(sales_bronze_df)

    customer_silver_df.write.csv(
        "./data/house/customer_info.csv", mode="overwrite", header=True
    )
    product_silver_df.write.csv(
        "./data/house/product_info.csv", mode="overwrite", header=True
    )
    sales_silver_df.write.csv(
        "./data/house/sales_info.csv", mode="overwrite", header=True
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Pyspark ETL Learning").getOrCreate()

    run_pipeline(spark)

    spark.stop()
