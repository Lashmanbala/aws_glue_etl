from iam_util import create_iam_policy, create_iam_role
from glue_util import create_glue_crawler, start_glue_crawler, create_glue_job, run_glue_job, create_workflow, create_on_demand_trigger,create_conditional_trigger, start_workflow
import dotenv

def main():
    bucket_name = 'github-activity-bucket-123'

    policy_ARN = ['arn:aws:iam::aws:policy/AmazonS3FullAccess']

    res = create_iam_role(policy_ARN)
    role_ARN = res['Role']['Arn']
    print(res)

    crawler_NAME = 'gha_crawler'
    s3_path = f's3://{bucket_name}/landing/'
    db_name = 'gha_database'
    prefix = 'gha_' 

    create_crawler_res = create_glue_crawler(crawler_NAME, role_ARN, db_name, s3_path, prefix)
    print(create_crawler_res)

    job_name = 'gha_glue_job3'
    script_location = f's3://{bucket_name}/scripts/glue_script.py'  # S3 path for the Glue script
    temp_dir = f's3://{bucket_name}/temp/'  # Temporary directory for Glue

    res = create_glue_job(job_name, role_ARN, script_location, temp_dir)
    print('Job created')
    print(res)

    crawler_NAME = 'gha_parquet_crawler'
    s3_path = f's3://{bucket_name}/cleaned/'

    res = create_glue_crawler(crawler_NAME, role_ARN, db_name, s3_path, prefix)
    print(res)

    workflow_name = 'gha_workflow'

    create_workflow_res = create_workflow(workflow_name)
    print('Workflow created')
    print(create_workflow_res)

    trigger_name = 'glue_job_trigger2'
    job_name = 'gha_glue_job3'
    workflow_name = 'gha_workflow'
    job_arguments = {
        '--SRC_BUCKET_NAME': 'github-activity-bucket-123',
        '--SRC_FOLDER_NAME': 'landing',
        '--TGT_BUCKET_NAME': 'github-activity-bucket-123',
        '--TGT_FOLDER_NAME': 'cleaned'
    }

    create_trigger_res = create_on_demand_trigger(trigger_name, workflow_name, job_name, job_arguments)
    print('On-demand trigger created')
    print(create_trigger_res)

    trigger_name = 'parquet_crawler_trigger2'
    crawler_name = 'gha_parquet_crawler'

    create_trigger_response = create_conditional_trigger(trigger_name, workflow_name, job_name, crawler_name)
    print('conditional trigger created')
    print(create_trigger_response)

    workflow_res = start_workflow(workflow_name)
    print('Workflow started')
    print(workflow_res)

if __name__ == "__main__":

    dotenv.load_dotenv()

    main()



