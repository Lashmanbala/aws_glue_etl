from iam_util import create_iam_policy, create_iam_role
from glue_util import create_glue_crawler, start_glue_crawler
import dotenv

dotenv.load_dotenv()

bucket_name = 'github-activity-bucket'

policy_ARN = create_iam_policy(bucket_name)

res = create_iam_role(policy_ARN)
role_ARN = res['Role']['Arn']
print(res)

# role_ARN = 'arn:aws:iam::872515260721:role/service-role/AWSGlueServiceRole-GHactivity'

crawler_NAME = 'gha_crawler'
s3_path = f's3://{bucket_name}/landing/'
db_name = 'gha_database'
prefix = 'gha_' 

res = create_glue_crawler(crawler_NAME, role_ARN, db_name, s3_path, prefix)
print(res)

start_res = start_glue_crawler(crawler_NAME)
print(start_res)