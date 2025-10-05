import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import to_timestamp, col
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read the parquet data from S3
S3_PROCESSED_PATH = "s3://glue-hardware-correlation/processed/"
processed_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [S3_PROCESSED_PATH]},
    format="parquet",
    transformation_ctx="processed_dyf"
)

# Convert to Spark DataFrame for type casting
df = processed_dyf.toDF()

# Explicitly cast error_timestamp from string → timestamp
df = df.withColumn("error_timestamp", to_timestamp(col("error_timestamp"), "yyyy-MM-dd HH:mm:ss"))

# Convert back to DynamicFrame for Glue JDBC write
processed_dyf = DynamicFrame.fromDF(df, glueContext, "processed_dyf")

# Write to Postgres
glueContext.write_dynamic_frame.from_options(
    frame=processed_dyf,
    connection_type="jdbc",
    connection_options={
        "url": "jdbc:postgresql://hardware-correlation-dw.cruwk6y2wzk5.eu-west-2.rds.amazonaws.com:5432/postgres",
        "dbtable": "public.hardware_error_analysis",
        "user": "glue_admin",
        "password": "glue_admin_pass",
        "batchsize": "5000"
    },
    transformation_ctx="write_to_rds"
)

job.commit()
