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

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1707921794781 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://bucket-misia-chrupek-excercises/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedZone_node1707921794781",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1707921631380 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1707921631380",
)

# Script generated for node Join
Join_node1707921675886 = Join.apply(
    frame1=CustomerTrustedZone_node1707921794781,
    frame2=AccelerometerLanding_node1707921631380,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1707921675886",
)

# Script generated for node Drop Fields
DropFields_node1707922612348 = DropFields.apply(
    frame=Join_node1707921675886,
    paths=[
        "phone",
        "email",
        "lastUpdateDate",
        "shareWithFriendsAsOfDate",
        "customerName",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "birthDay",
        "shareWithPublicAsOfDate",
        "serialNumber",
    ],
    transformation_ctx="DropFields_node1707922612348",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1707921695803 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1707922612348,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://bucket-misia-chrupek-excercises/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node1707921695803",
)

job.commit()
