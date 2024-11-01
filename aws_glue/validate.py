from athena_util import query_execution, get_query_results
import dotenv

def validate():
    bucket_name = 'github-activity-bucket'
    db_name = 'gha_database'

    query = 'SELECT COUNT(*) FROM gha_cleaned'
    s3_output_location = f's3://{bucket_name}/athena_query_results/'

    query_reponse = query_execution(db_name, query, s3_output_location)

    execution_id = query_reponse['QueryExecutionId']

    query_result = get_query_results(execution_id)
    res = query_result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']

    print(f'Total n.of records: {res}')

if __name__ == "__main__":

    dotenv.load_dotenv()

    validate()