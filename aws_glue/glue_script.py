import sys
import os
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import date_format, substring
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATABASE_NAME', 'TABLE_NAME', 'TGT_BUCKET_NAME', 'TGT_FOLDER_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Getting variables
database_name = args['DATABASE_NAME']
table_name = args['TABLE_NAME']
tgt_bucket_name = args['TGT_BUCKET_NAME']
tgt_folder_name = args['TGT_FOLDER_NAME']

# Reading data from glue table as dynamic_frame
datasource = glueContext.create_dynamic_frame.from_catalog(database = database_name,  # should be named args not positional args
                                                           table_name = table_name, 
                                                           transformation_ctx="datasource")

# Changing Schema
ChangeSchema_datasource = ApplyMapping.apply(frame=datasource, 
                                             mappings=[("id", "string", "id", "string"), ("type", "string", "type", "string"), ("actor.id", "int", "actor.id", "int"), ("actor.login", "string", "actor.login", "string"), ("actor.display_login", "string", "actor.display_login", "string"), ("actor.gravatar_id", "string", "actor.gravatar_id", "string"), ("actor.url", "string", "actor.url", "string"), ("actor.avatar_url", "string", "actor.avatar_url", "string"), ("repo.id", "int", "repo.id", "int"), ("repo.name", "string", "repo.name", "string"), ("repo.url", "string", "repo.url", "string"), ("payload.repository_id", "int", "payload.repository_id", "int"), ("payload.push_id", "long", "payload.push_id", "long"), ("payload.size", "int", "payload.size", "int"), ("payload.distinct_size", "int", "payload.distinct_size", "int"), ("payload.ref", "string", "payload.ref", "string"), ("payload.head", "string", "payload.head", "string"), ("payload.before", "string", "payload.before", "string"), ("payload.commits", "array", "payload.commits", "array"), ("payload.action", "string", "payload.action", "string"), ("payload.ref_type", "string", "payload.ref_type", "string"), ("payload.master_branch", "string", "payload.master_branch", "string"), ("payload.description", "string", "payload.description", "string"), ("payload.pusher_type", "string", "payload.pusher_type", "string"), ("payload.number", "int", "payload.number", "int"), ("payload.pages", "array", "payload.pages", "array"), ("public", "boolean", "public", "boolean"), ("created_at", "string", "created_at", "string"), ("org.id", "int", "org.id", "int"), ("org.login", "string", "org.login", "string"), ("org.gravatar_id", "string", "org.gravatar_id", "string"), ("org.url", "string", "org.url", "string"), ("org.avatar_url", "string", "org.avatar_url", "string")], transformation_ctx="ChangeSchema_datasource")

# Creating columns for partitioning with changing dynamic_frame into dataframe
df = ChangeSchema_datasource. \
  toDF(). \
  withColumn('year', date_format(substring('created_at', 1, 10), 'yyyy')). \
  withColumn('month', date_format(substring('created_at', 1, 10), 'MM')). \
  withColumn('day', date_format(substring('created_at', 1, 10), 'dd'))

# Converting dataframe back to dynamicframe  
dyf = DynamicFrame.fromDF(dataframe=df, glue_ctx=glueContext, name="dyf")

# writing partitioned data into s3 in Parquet format
dyf_partitioned = glueContext.write_dynamic_frame.from_options(frame=dyf, 
        connection_type="s3", 
        format="glueparquet", 
        connection_options={"path": f"s3://{tgt_bucket_name}/{tgt_folder_name}/", "partitionKeys": ["year", "month", "day"]}, 
        format_options={"compression": "snappy"}, transformation_ctx="dyf_partitioned")

job.commit()