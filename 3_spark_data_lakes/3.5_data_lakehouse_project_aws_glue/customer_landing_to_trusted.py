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

# Script generated for node Customer Landing
CustomerLanding_node1745616646050 = glueContext.create_dynamic_frame.from_catalog(database="db-milenko", table_name="customer_landing", transformation_ctx="CustomerLanding_node1745616646050")

# Script generated for node Share with Research
SqlQuery0 = '''
select * from myDataSource
where sharewithresearchasofdate is not null
'''
SharewithResearch_node1745615806051 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":CustomerLanding_node1745616646050}, transformation_ctx = "SharewithResearch_node1745615806051")

# Script generated for node Customer Trusted
EvaluateDataQuality().process_rows(frame=SharewithResearch_node1745615806051, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745611095929", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrusted_node1745617173983 = glueContext.getSink(path="s3://bucket-milenko/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1745617173983")
CustomerTrusted_node1745617173983.setCatalogInfo(catalogDatabase="db-milenko",catalogTableName="customer_trusted")
CustomerTrusted_node1745617173983.setFormat("json")
CustomerTrusted_node1745617173983.writeFrame(SharewithResearch_node1745615806051)
job.commit()