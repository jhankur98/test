import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
sourceData = glueContext.create_data_frame.from_catalog( \
    database = "hudidb", \
    table_name = "manish_schema_registry", \
    transformation_ctx = "sourceData", \
    additional_options = {"startingPosition": "TRIM_HORIZON", "inferSchema": "true"})
    
sourceData.printSchema()
    
AWSGlueDataCatalog_node1699492978719 = glueContext.create_dynamic_frame.from_catalog(
    database="hudidb",
    table_name="manish_schema_registry",
    transformation_ctx="AWSGlueDataCatalog_node1699492978719",
)
print("==================")
AWSGlueDataCatalog_node1699492978719.printSchema()

job.commit()
