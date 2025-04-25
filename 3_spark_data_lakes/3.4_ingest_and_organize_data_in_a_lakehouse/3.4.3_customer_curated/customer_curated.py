import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1745312046534 = glueContext.create_dynamic_frame.from_catalog(database="db-milenko", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1745312046534")

# Script generated for node Customer Trusted
CustomerTrusted_node1745312166890 = glueContext.create_dynamic_frame.from_catalog(database="db-milenko", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1745312166890")

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node1745312225871 = Join.apply(frame1=AccelerometerLanding_node1745312046534, frame2=CustomerTrusted_node1745312166890, keys1=["user"], keys2=["email"], transformation_ctx="CustomerPrivacyFilter_node1745312225871")

# Script generated for node Drop Fields and Duplicates
SqlQuery1023 = '''
select distinct customername, email, phone, birthday, 
serialnumber, registrationdate, lastupdatedate, 
sharewithresearchasofdate, sharewithpublicasofdate, 
sharewithfriendsasofdate from myDataSource
'''
DropFieldsandDuplicates_node1745343310599 = sparkSqlQuery(glueContext, query = SqlQuery1023, mapping = {"myDataSource":CustomerPrivacyFilter_node1745312225871}, transformation_ctx = "DropFieldsandDuplicates_node1745343310599")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=DropFieldsandDuplicates_node1745343310599, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1744469747019", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1744471582136 = glueContext.getSink(path="s3://bucket-milenko/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1744471582136")
CustomerCurated_node1744471582136.setCatalogInfo(catalogDatabase="db-milenko",catalogTableName="customer_curated")
CustomerCurated_node1744471582136.setFormat("json")
CustomerCurated_node1744471582136.writeFrame(DropFieldsandDuplicates_node1745343310599)
job.commit()