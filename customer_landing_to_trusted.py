import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 Bucket
S3Bucket_node1685352206564 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://bucket-misia-chrupek-excercises/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3Bucket_node1685352206564",
)

# Script generated for node Privacy Filter
PrivacyFilter_node1707757771055 = Filter.apply(
    frame=S3Bucket_node1685352206564,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="PrivacyFilter_node1707757771055",
)

# Script generated for node Trusted Customer Zone
TrustedCustomerZone_node1707757806803 = glueContext.getSink(
    path="s3://bucket-misia-chrupek-excercises/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="TrustedCustomerZone_node1707757806803",
)
TrustedCustomerZone_node1707757806803.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
TrustedCustomerZone_node1707757806803.setFormat("json")
TrustedCustomerZone_node1707757806803.writeFrame(PrivacyFilter_node1707757771055)
job.commit()
