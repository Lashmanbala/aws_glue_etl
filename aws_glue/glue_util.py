import boto3
import time

def create_glue_crawler(crawler_name, role, database_name, s3_target_path, table_prefix):
    glue_client = boto3.client('glue')

    res = glue_client.create_crawler(
            Name=crawler_name,
            Role=role,
            DatabaseName=database_name,
            Targets={
                'S3Targets': [
                    {
                        'Path': s3_target_path
                    }
                ]
            },
            TablePrefix=table_prefix, 
            Description='Crawler to crawl data in S3 and populate Glue Data Catalog',
            RecrawlPolicy={
                'RecrawlBehavior': 'CRAWL_EVERYTHING'
                }
            # , Schedule='cron(0 12 * * ? *)',  # Optional schedule for periodic runs

            )
    time.sleep(5) # wait untill crawler created
    return res

def start_glue_crawler(crawler_name):
    glue_client = boto3.client('glue')

    res = glue_client.start_crawler(Name=crawler_name)

    return res

def create_glue_job(job_name, iam_role, script_location, temp_dir):
    glue_client = boto3.client('glue')

    response = glue_client.create_job(
        Name=job_name,
        Role=iam_role,
        Command={
            'Name': 'glueetl',  # Specifies this is a Glue ETL job
            'ScriptLocation': script_location,
            'PythonVersion': '3'  # Python version (2 or 3)
        },
        GlueVersion='4.0',  # Optional: Glue version
        # MaxCapacity=max_capacity,  # Specifies the number of DPUs to allocate 
        # Need not to set max capacity if n.of workers and worker type is set
        NumberOfWorkers=4,
        WorkerType='Standard',
        DefaultArguments={
            '--TempDir': temp_dir,
            '--job-bookmark-option': 'job-bookmark-enable',
        },
        Description='github activity data transformation job',
        ExecutionProperty={
            'MaxConcurrentRuns': 1  # Limits the job to one concurrent run
        },
        Timeout=60,  # Timeout in minutes
        MaxRetries=1,  # Retry number # Meaning totally 2 retries: retry-0 and retry-1.
    )
    time.sleep(5) # wait untill job created

    return response

def run_glue_job(job_name, database_name, table_name, bucket_name, folder_name):
    glue_client = boto3.client('glue')

    res = glue_client.start_job_run(JobName=job_name,
                                       Arguments={                # Setting up env variables
                                     '--DATABASE_NAME': f'{database_name}',
                                    '--TABLE_NAME': f'{table_name}',
                                    '--TGT_BUCKET_NAME':  f'{bucket_name}',
                                    '--TGT_FOLDER_NAME': F'{folder_name}'
                                    })

    return res
