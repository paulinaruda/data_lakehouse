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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1707938004224 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://bucket-misia-chrupek-excercises/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1707938004224",
)

# Script generated for node Customer Curated
CustomerCurated_node1707938061019 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://bucket-misia-chrupek-excercises/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1707938061019",
)

# Script generated for node Privacy Filter
PrivacyFilter_node1707938085712 = Join.apply(
    frame1=CustomerCurated_node1707938061019,
    frame2=StepTrainerLanding_node1707938004224,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="PrivacyFilter_node1707938085712",
)

# Script generated for node Drop Fields
DropFields_node1707938957362 = DropFields.apply(
    frame=PrivacyFilter_node1707938085712,
    paths=[
        "shareWithPublicAsOfDate",
        "phone",
        "lastUpdateDate",
        "shareWithFriendsAsOfDate",
        "customerName",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "birthDay",
        "serialNumber",
        "email",
    ],
    transformation_ctx="DropFields_node1707938957362",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1707938134521 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1707938957362,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://bucket-misia-chrupek-excercises/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node1707938134521",
)

job.commit()
