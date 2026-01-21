from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
    DateType,
)

customer_info_bronze_schema = schema = StructType(
    [
        StructField("cst_id", StringType()),
        StructField("cst_key", StringType()),
        StructField("cst_firstname", StringType()),
        StructField("cst_lastname", StringType()),
        StructField("cst_marital_status", StringType()),
        StructField("cst_gndr", StringType()),
        StructField("cst_create_date", DateType()),
    ]
)

product_info_bronze_schema = StructType(
    [
        StructField("prd_id", IntegerType()),
        StructField("prd_key", StringType()),
        StructField("prd_nm", StringType()),
        StructField("prd_cost", DoubleType()),
        StructField("prd_line", StringType()),
        StructField("prd_start_dt", DateType()),
        StructField("prd_end_dt", DateType()),
    ]
)

sales_info_bronze_schema = StructType(
    [
        StructField("sls_ord_num", StringType()),
        StructField("sls_prd_key", StringType()),
        StructField("sls_cust_id", IntegerType()),
        StructField("sls_order_dt", IntegerType()),
        StructField("sls_ship_dt", IntegerType()),
        StructField("sls_due_dt", IntegerType()),
        StructField("sls_sales", DoubleType()),
        StructField("sls_quantity", IntegerType()),
        StructField("sls_price", DoubleType()),
    ]
)
