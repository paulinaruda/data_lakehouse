import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1707921794781 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://bucket-misia-chrupek-excercises/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1707921794781",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1707921631380 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1707921631380",
)

# Script generated for node Join
Join_node1707921675886 = Join.apply(
    frame1=CustomerTrusted_node1707921794781,
    frame2=AccelerometerTrusted_node1707921631380,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1707921675886",
)

# Script generated for node Drop Fields
DropFields_node1707922612348 = DropFields.apply(
    frame=Join_node1707921675886,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1707922612348",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1707931278524 = DynamicFrame.fromDF(
    DropFields_node1707922612348.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1707931278524",
)

# Script generated for node Customer Curated
CustomerCurated_node1707921695803 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1707931278524,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://bucket-misia-chrupek-excercises/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node1707921695803",
)

job.commit()
