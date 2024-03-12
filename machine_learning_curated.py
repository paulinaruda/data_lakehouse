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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1707941142694 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://bucket-misia-chrupek-excercises/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1707941142694",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1707941133727 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://bucket-misia-chrupek-excercises/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1707941133727",
)

# Script generated for node Join
Join_node1707941146164 = Join.apply(
    frame1=StepTrainerTrusted_node1707941142694,
    frame2=AccelerometerTrusted_node1707941133727,
    keys1=["sensorReadingTime"],
    keys2=["timestamp"],
    transformation_ctx="Join_node1707941146164",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1707941153237 = DynamicFrame.fromDF(
    Join_node1707941146164.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1707941153237",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1707941156748 = glueContext.getSink(
    path="s3://bucket-misia-chrupek-excercises/machinelearning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node1707941156748",
)
MachineLearningCurated_node1707941156748.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node1707941156748.setFormat("json")
MachineLearningCurated_node1707941156748.writeFrame(DropDuplicates_node1707941153237)
job.commit()
