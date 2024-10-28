from iam_util import create_iam_policy, create_iam_role
from glue_util import create_glue_crawler, start_glue_crawler, create_glue_job, run_glue_job
from athena_util import query_execution, get_query_results
import dotenv

dotenv.load_dotenv()

bucket_name = 'github-activity-bucket'

# policy_ARN = create_iam_policy(bucket_name)

# res = create_iam_role(policy_ARN)
# role_ARN = res['Role']['Arn']
# print(res)

# crawler_NAME = 'gha_crawler'
# role_ARN = 'arn:aws:iam::872515260721:role/service-role/AWSGlueServiceRole-GHactivity'
# s3_path = f's3://{bucket_name}/landing/'
db_name = 'gha_database'
prefix = 'gha_' 

# res = create_glue_crawler(crawler_NAME, role_ARN, db_name, s3_path, prefix)
# print(res)

# start_res = start_glue_crawler(crawler_NAME)
# print(start_res)

job_name = 'gh-etl-job'
role_ARN = 'arn:aws:iam::872515260721:role/service-role/AWSGlueServiceRole-GHactivity'
script_location = f's3://{bucket_name}/scripts/glue_script.py'  # S3 path for the Glue script
temp_dir = f's3://{bucket_name}/temp/'  # Temporary directory for Glue

# res = create_glue_job(job_name, role_ARN, script_location, temp_dir)

# print(res)

src_bucket_name = bucket_name
src_folder_name = 'landing'
tgt_bucket_name = bucket_name
tgt_folder_name = 'cleaned'

run_response = run_glue_job(job_name, src_bucket_name, src_folder_name, tgt_bucket_name, tgt_folder_name)
print(run_response)

# crawler_NAME = 'gha_parquet_crawler'
# s3_path = f's3://{bucket_name}/cleaned_parquet/'
db_name = 'gha_database'
# prefix = 'gha_' 

# res = create_glue_crawler(crawler_NAME, role_ARN, db_name, s3_path, prefix)
# print(res)

# start_res = start_glue_crawler(crawler_NAME)
# print(start_res)

# query = 'SELECT COUNT(*) FROM gha_cleaned_parquet'
# s3_output_location = f's3://{bucket_name}/athena_query_results/'

# query_reponse = query_execution(db_name, query, s3_output_location)
# print(query_reponse)

# execution_id = query_reponse['QueryExecutionId']

# query_result = get_query_results(execution_id)
# print(f'Total n.of records: {query_result}')
# res = query_result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']
# print(f'Total n.of records: {res}')


# Total n.of records: 4974439

# AmazonS3_node1730008836061 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "true"}, connection_type="s3", format="json", connection_options={"paths": ["s3://github-activity-bucket/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1730008836061")

'''
    "Failure Reason": "Traceback (most recent call last):\n  
File \"/tmp/glue_script.py\", line 40, in <module>\n    
withColumn('year', date_format(substring('created_at', 1, 10), 'yyyy')). \\\n 
File \"/opt/amazon/spark/python/lib/pyspark.zip/pyspark/sql/dataframe.py\", line 3037, in withColumn\n    
return DataFrame(self._jdf.withColumn(colName, col._jc), self.sparkSession)\n 
File \"/opt/amazon/spark/python/lib/py4j-0.10.9.5-src.zip/py4j/java_gateway.py\", line 1321, in __call__\n   
return_value = get_return_value(\n 
 File \"/opt/amazon/spark/python/lib/pyspark.zip/pyspark/sql/utils.py\", line 196, in deco\n    
 raise converted from None\npyspark.sql.utils.AnalysisException: Column 'created_at' does not exist. 
 Did you mean one of the following? [];\n
 'Project [date_format(substring('created_at, 1, 10), yyyy, Some(UTC)) AS year#0]\n
+- LogicalRDD false\n\nSpark Error Class: MISSING_COLUMN; Traceback (most recent call last):\n  
File \"/tmp/glue_script.py\", line 40, in <module>\n    withColumn('year', date_format(substring('created_at', 1, 10), 'yyyy')). \\\n  
File \"/opt/amazon/spark/python/lib/pyspark.zip/pyspark/sql/dataframe.py\", line 3037, in withColumn\n    
return DataFrame(self._jdf.withColumn(colName, col._jc), self.sparkSession)\n  
File \"/opt/amazon/spark/python/lib/py4j-0.10.9.5-src.zip/py4j/java_gateway.py\", line 1321, in __call__\n    
return_value = get_return_value(\n  
File \"/opt/amazon/spark/python/lib/pyspark.zip/pyspark/sql/utils.py\", line 196, in deco\n   
 raise converted from None\npyspark.sql.utils.AnalysisException: Column 'created_at' does not exist. Did you mean one of the following? [];\n
 'Project [date_format(substring('created_at, 1, 10), yyyy, Some(UTC)) AS year#0]\n+- LogicalRDD false\n",
'''

'''
bala@Bala:~/code/projects/glue_etl$ aws glue get-job-bookmark --job-name gha_job1
{
    "JobBookmarkEntry": {
        "JobName": "gha_job1",
        "Version": 2,
        "Run": 2,
        "Attempt": 0,
        "RunId": "jr_16cd15a400d35a3ea7351cc32280ae213d3ab051f4058f5507044616471d02d7",
        "JobBookmark": "{\"AWSGlueDataCatalog_node1730078287682\":{\"jsonClass\":\"HadoopDataSourceJobBookmarkState\",\"timestamps\":{\"RUN\":\"1\",\"HIGH_BAND\":\"900000\",\"CURR_LATEST_PARTITION\":\"0\",\"CURR_LATEST_PARTITIONS\":\"\",\"CURR_RUN_START_TIME\":\"2024-10-28T01:32:45.613Z\",\"INCLUDE_LIST\":\"\"}}}"
    }
}'''

# 4974439
# 9805650

'''
bala@Bala:~/code/projects/glue_etl$ aws glue get-job-bookmark --job-name gha_job1
{
    "JobBookmarkEntry": {
        "JobName": "gha_job1",
        "Version": 4,
        "Run": 3,
        "Attempt": 0,
        "PreviousRunId": "jr_16cd15a400d35a3ea7351cc32280ae213d3ab051f4058f5507044616471d02d7",
        "RunId": "jr_6d3a4e661a941a4ad8ed003c7a933a9b8f1dbbd5e23288d768d7ede301065eeb",
        "JobBookmark": "{\"AWSGlueDataCatalog_node1730078287682\":{\"jsonClass\":\"HadoopDataSourceJobBookmarkState\",\"timestamps\":
        {\"RUN\":\"2\",\"HIGH_BAND\":\"900000\",\"CURR_LATEST_PARTITION\":\"0\",\"CURR_LATEST_PARTITIONS\":\"\",\"CURR_RUN_START_TIME\":
        \"2024-10-28T02:12:06.503Z\",\"INCLUDE_LIST\":\"f437cb0563bdfffd53da1a73762abf0e,5383de8c63a953499ae982cc067dd29e,
        eeadf7ffc60fc409ebac38aa1e603ca7,e19f459dc7d51fb981b69127ed8ff2db,bdc8e044b1a2af7fdb3b52870aeb02c0,8b7df3ed4aecec850fcb518be54ba81b,
        acc9e2ed056c2d6d80b8abe97cdf44aa,833e7abda36954c63f867a3214fd16fb,eb2d3a4e4907ee0509bacb422456b4e1,57fd18c45402cb49f89311baee78d5bb,
        8e44c6b4d9618fcc9155b70f479ce688,8ef4cc7843c8792625ee80bef181f90d,84a3af8f940f642af12558be61b7ae3b,9963f53dd31d8975a2e94594c71498fb,
        6f9e6eb2aacb6122cb52f5230f4e66f4,28a5e953f8db056920c15e79196cd11f,7f36d16aed769368ff859bf630da0926,6031b0c95af4693d3b8adedec745b388,
        bcf7c27ab451fd9256a8d4d276c67879,d1d1a077928f2b4c922de9e4ed2417ed,b4e3da25fbb56dba94e874711b7f766d,d591c03a2184e900b466af7e72da2860,
        80a3d2cc54008a4b4aee2d52aaba1c4e,b6867e94a5ff3e976f488a33115e8cc0,383b7757b04452da06945c68cd3be6c6,96ed19593fb4262b82779afa5b4219b9,
        4985e4cbfe326d9a55bc47afe7399386,31d27f04eba03852a3d6a1552aef8a49,3cb9b793f8c547d5dfa3b2d5088dafab,aa1e0391eda6e73034bab2d028c8cf66,
        4e10a779af7c41f48f07dde96932fd3e,f3191791a6c58abf13f8cd5ff4c08c6d,35e97f5c401c8271df9d2976f318bceb,1327a12b82573d0d47d5de331b056097,
        492f9eff5ba14ee8bf1e917aa13935a4,d7caed73d1914f54f9bb37deb96f08d1,4c86bc3cac52e9846ada1e6dae920f66,10e73569d08ee77da11d41a6d9592d9a,
        9d8e4cf76da826ec9bc9a021984eb96d,a8612664c71a9b8b04c7cf397027c80,5ff7b6b1edbec4ad590c42a93530d0e1,d5e1b1b0caa5a7a97bd2efcb3c31490a,
        9e069e8e3c832e4ea7a912b51e1caa05,33cff46ece4ef9f0db757be753d903f1,80de35b535afdd776d7995b5c6c9496,d7ee7e102329015f08adc94b1a045cc4,
        1c87da5e5a4d79cee2c037df3987bcf1,19cd71eb2b70718ec9fe0d3650bc8f16\"}}}"
    }'''
