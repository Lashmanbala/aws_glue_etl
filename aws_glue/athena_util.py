import boto3
import time

def query_execution(database, query, s3_output_location):
    athena_client = boto3.client('athena')

    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database  # Glue database name
        },
        ResultConfiguration={
            'OutputLocation': s3_output_location
        }
    )
    
    time.sleep(10) # wait untill the qury run
    return response

def get_query_results(execution_id):
    athena_client = boto3.client('athena')

    results = athena_client.get_query_results(QueryExecutionId=execution_id)
    return results