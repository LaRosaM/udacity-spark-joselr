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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="joselr-spark",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1693016303704 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-spark-joselr/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedZone_node1693016303704",
)

# Script generated for node Join Customer with Permissions
JoinCustomerwithPermissions_node1693016337513 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerTrustedZone_node1693016303704,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomerwithPermissions_node1693016337513",
)

# Script generated for node Drop Fields
DropFields_node1693016611998 = DropFields.apply(
    frame=JoinCustomerwithPermissions_node1693016337513,
    paths=["z", "y", "x", "timestamp", "user"],
    transformation_ctx="DropFields_node1693016611998",
)

# Script generated for node Customer Curated
CustomerCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1693016611998,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-spark-joselr/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node3",
)

job.commit()
