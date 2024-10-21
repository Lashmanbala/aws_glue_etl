import boto3

def create_glue_crawler(crawler_name, role, database_name, s3_target_path, table_prefix):
    glue_client = boto3.client('glue')

    glue_client.create_crawler(
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

def start_glue_crawler(crawler_name):
    glue_client = boto3.client('glue')

    glue_client.start_crawler(Name=crawler_name)
