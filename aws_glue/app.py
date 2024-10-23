from iam_util import create_iam_policy, create_iam_role
from glue_util import create_glue_crawler, start_glue_crawler, create_glue_job, run_glue_job
import dotenv

dotenv.load_dotenv()

bucket_name = 'github-activity-bucket'

policy_ARN = create_iam_policy(bucket_name)

res = create_iam_role(policy_ARN)
role_ARN = res['Role']['Arn']
print(res)

crawler_NAME = 'gha_crawler'
s3_path = f's3://{bucket_name}/landing/'
db_name = 'gha_database'
prefix = 'gha_' 

res = create_glue_crawler(crawler_NAME, role_ARN, db_name, s3_path, prefix)
print(res)

start_res = start_glue_crawler(crawler_NAME)
print(start_res)

job_name = 'gha-job'
role_ARN = 'arn:aws:iam::872515260721:role/service-role/AWSGlueServiceRole-GHactivity'
script_location = f's3://{bucket_name}/scripts/glue_script.py'  # S3 path for the Glue script
temp_dir = f's3://{bucket_name}/temp/'  # Temporary directory for Glue

res = create_glue_job(job_name, role_ARN, script_location, temp_dir)

print(res)

# table_name = f'{prefix}landing'
table_name = 'gha_landing_landing'
folder_name = 'cleaned_parquet'

run_response = run_glue_job(job_name, db_name, table_name, bucket_name, folder_name)
print(run_response)


crawler_NAME = 'gha_parquet_crawler'
s3_path = f's3://{bucket_name}/cleaned_parquet/'
db_name = 'gha_database'
prefix = 'gha_' 

res = create_glue_crawler(crawler_NAME, role_ARN, db_name, s3_path, prefix)
print(res)

start_res = start_glue_crawler(crawler_NAME)
print(start_res)