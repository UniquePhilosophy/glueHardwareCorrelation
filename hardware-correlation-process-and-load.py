import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, current_date, datediff

# --- Boilerplate Initialization ---
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --- Data Ingestion from Glue Data Catalog ---
# We read data using the table names created by our crawler. This is a best practice.
# It decouples the ETL script from the physical storage location.

logs_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="lumos_db", table_name="logs", transformation_ctx="logs_dyf"
)

sales_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="lumos_db", table_name="sales", transformation_ctx="sales_dyf"
)

customers_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="lumos_db", table_name="sales_1", transformation_ctx="customers_dyf" # Note: Glue crawler might name it sales_1 if it's in the same folder
)

# --- Data Transformation & Cleaning ---
# Convert DynamicFrames to Spark DataFrames for more powerful transformations using Spark SQL functions.

logs_df = logs_dyf.toDF()
sales_df = sales_dyf.toDF()
customers_df = customers_dyf.toDF()

# Filter for only critical events like 'ERROR' to focus the analysis
error_logs_df = logs_df.filter(col("event_type") == "ERROR")

# Feature Engineering: Calculate how many days ago the device was sold.
# This could be a useful feature for analysis (e.g., do newer devices fail more?).
sales_df = sales_df.withColumn("sale_date_dt", to_date(col("sale_date"), "yyyy-MM-dd"))
sales_df = sales_df.withColumn("days_since_sale", datediff(current_date(), col("sale_date_dt")))


# --- Joining DataFrames to create a unified view ---
# This is the core of our transformation, combining operational and business data.

# Join error logs with sales data on the device_id
enriched_errors_df = error_logs_df.join(sales_df, "device_id", "inner")

# Join the result with customer data on the customer_id
final_df = enriched_errors_df.join(customers_df, "customer_id", "inner")

# --- Schema Finalization ---
# Select and rename columns for a clean, final schema for our data warehouse.

output_df = final_df.select(
    col("log_id"),
    col("device_id"),
    col("timestamp").alias("error_timestamp"),
    col("cpu_temp_celsius"),
    col("memory_usage_gb"),
    col("sale_id"),
    col("days_since_sale"),
    col("customer_name"),
    col("industry")
)

# --- Load Data to Processed S3 Layer in Parquet Format ---
# Parquet is a columnar storage format optimized for analytical queries.
# It's a standard practice for data warehousing staging layers.

S3_PROCESSED_PATH = "s3://disguise-project-lumos-yourname-2025/processed/"

output_df.write.mode("overwrite").parquet(S3_PROCESSED_PATH)

job.commit()