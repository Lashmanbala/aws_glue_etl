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
# s3_path = f's3://{bucket_name}/landing/'
# db_name = 'gha_database'
# prefix = 'gha_' 

# res = create_glue_crawler(crawler_NAME, role_ARN, db_name, s3_path, prefix)
# print(res)

# start_res = start_glue_crawler(crawler_NAME)
# print(start_res)

# job_name = 'gha-job'
# role_ARN = 'arn:aws:iam::872515260721:role/service-role/AWSGlueServiceRole-GHactivity'
# script_location = f's3://{bucket_name}/scripts/glue_script.py'  # S3 path for the Glue script
# temp_dir = f's3://{bucket_name}/temp/'  # Temporary directory for Glue

# res = create_glue_job(job_name, role_ARN, script_location, temp_dir)

# print(res)

# # table_name = f'{prefix}landing'
# table_name = 'gha_landing_landing'
# folder_name = 'cleaned_parquet'

# run_response = run_glue_job(job_name, db_name, table_name, bucket_name, folder_name)
# print(run_response)

# crawler_NAME = 'gha_parquet_crawler'
# s3_path = f's3://{bucket_name}/cleaned_parquet/'
db_name = 'gha_database'
# prefix = 'gha_' 

# res = create_glue_crawler(crawler_NAME, role_ARN, db_name, s3_path, prefix)
# print(res)

# start_res = start_glue_crawler(crawler_NAME)
# print(start_res)

query = 'SELECT COUNT(*) FROM gha_cleaned_parquet'
s3_output_location = f's3://{bucket_name}/athena_query_results/'

query_reponse = query_execution(db_name, query, s3_output_location)
print(query_reponse)

execution_id = query_reponse['QueryExecutionId']

query_result = get_query_results(execution_id)
print(f'Total n.of records: {query_result}')
res = query_result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']
print(f'Total n.of records: {res}')
