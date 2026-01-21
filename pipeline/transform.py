from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from pyspark.sql.functions import (
    trim,
    col,
    upper,
    when,
    substring,
    to_date,
    length,
    regexp_replace,
)


def trim_col_names(spark_df: DataFrame) -> DataFrame:
    for field in spark_df.schema.fields:
        if isinstance(field.dataType, StringType):
            spark_df = spark_df.withColumn(field.name, trim(col(field.name)))

    return spark_df


def rename_dataframe_cols(spark_df: DataFrame, rename_map: dict[str, str]) -> DataFrame:
    for old_name, new_name in rename_map.items():
        spark_df = spark_df.withColumnRenamed(old_name, new_name)

    return spark_df


def transform_customer_info(spark_df: DataFrame) -> DataFrame:
    spark_df = trim_col_names(spark_df)
    spark_df = spark_df.filter(col("cst_id").isNotNull())

    RENAME_MAP = {
        "cst_id": "customer_id",
        "cst_key": "customer_number",
        "cst_firstname": "first_name",
        "cst_lastname": "last_name",
        "cst_marital_status": "marital_status",
        "cst_gndr": "gender",
        "cst_create_date": "created_date",
    }
    spark_df = rename_dataframe_cols(spark_df, RENAME_MAP)

    spark_df = spark_df.withColumn(
        "marital_status",
        when(upper(col("marital_status")) == "S", "Single")
        .when(upper(col("marital_status")) == "M", "Married")
        .otherwise("n/a"),
    ).withColumn(
        "gender",
        when(upper(col("gender")) == "F", "Female")
        .when(upper(col("gender")) == "M", "Male")
        .otherwise("n/a"),
    )

    return spark_df


def transform_product_info(spark_df: DataFrame) -> DataFrame:
    spark_df = trim_col_names(spark_df)

    spark_df = spark_df.withColumn(
        "cat_id", regexp_replace(substring(col("prd_key"), 1, 5), "-", "_")
    )
    spark_df = spark_df.withColumn(
        "prd_key", substring(col("prd_key"), 7, length(col("prd_key")))
    )

    RENAME_MAP = {
        "prd_id": "product_id",
        "cat_id": "category_id",
        "prd_key": "product_number",
        "prd_nm": "product_name",
        "prd_cost": "product_cost",
        "prd_line": "product_line",
        "prd_start_dt": "start_date",
        "prd_end_dt": "end_date",
    }

    spark_df = rename_dataframe_cols(spark_df, RENAME_MAP)

    spark_df = spark_df.withColumn(
        "product_line",
        when(upper(col("product_line")) == "M", "Mountain")
        .when(upper(col("product_line")) == "R", "Road")
        .when(upper(col("product_line")) == "S", "Other Sales")
        .when(upper(col("product_line")) == "T", "Touring")
        .otherwise("n/a"),
    )

    return spark_df


def transform_sales_info(spark_df: DataFrame) -> DataFrame:
    spark_df = trim_col_names(spark_df)

    RENAME_MAP = {
        "sls_ord_num": "order_number",
        "sls_prd_key": "product_number",
        "sls_cust_id": "customer_id",
        "sls_order_dt": "order_date",
        "sls_ship_dt": "ship_date",
        "sls_due_dt": "due_date",
        "sls_sales": "sales_amount",
        "sls_quantity": "quantity",
        "sls_price": "price",
    }

    spark_df = rename_dataframe_cols(spark_df, RENAME_MAP)

    spark_df = (
        spark_df.withColumn(
            "order_date",
            when(
                (col("order_date") == 0) | (length(col("order_date")) != 8), None
            ).otherwise(to_date(col("order_date").cast("string"), "yyyyMMdd")),
        )
        .withColumn(
            "ship_date",
            when(
                (col("ship_date") == 0) | (length(col("ship_date")) != 8), None
            ).otherwise(to_date(col("ship_date").cast("string"), "yyyyMMdd")),
        )
        .withColumn(
            "due_date",
            when(
                (col("due_date") == 0) | (length(col("due_date")) != 8), None
            ).otherwise(to_date(col("due_date").cast("string"), "yyyyMMdd")),
        )
        .withColumn(
            "price",
            when(
                (col("price").isNull()) | (col("price") <= 0),
                when(
                    col("quantity") != 0, col("sales_amount") / col("quantity")
                ).otherwise(None),
            ).otherwise(col("price")),
        )
    )

    return spark_df
