# AWS Glue ETL

## Overview
- In this project, we extract the data from the web archive using aws lambda and store it in s3. 
- Using glue, the raw data is processed(cleaned, partitioned, converted to parquet) and stored it in s3.
- Using athena the cleand data is being queried.

## Architecture
- **S3** for data storage
- **AWS Glue Data Catalog** for metadata management
- **Glue ETL** for data processing
- **AWS Athena** for querying
- **IAM** for security and access management

## Setup
To set up and deploy this solution, follow these steps:


### Installation
1. Clone this repository:
    ```bash
    git clone https://github.com/Lashmanbala/aws_glue_etl.git
    ```
2. Configure your AWS environment and IAM roles.

3. Upload any necessary datasets to the specified S3 bucket.

4. Add Glue job scripts to the AWS Glue Console or initiate them with the Glue CLI.

## ETL Jobs
This repository includes the following ETL jobs:
1. **Data Extraction Job**: Reads data from a specified source (e.g., S3).
2. **Data Transformation Job**: Processes and transforms the extracted data.
3. **Data Load Job**: Writes the transformed data to a destination (e.g., Redshift or RDS).

Each job uses PySpark scripts, optimized for processing large datasets.

## Usage
To execute the ETL jobs:
1. Navigate to the AWS Glue Console.
2. Select the desired job and start it, or use the CLI:
    ```bash
    aws glue start-job-run --job-name <job_name>
    ```

3. Monitor the job progress in the Glue Console.

### Scheduling
You can schedule jobs by setting up triggers in AWS Glue or using AWS CloudWatch Events for automation.

## Troubleshooting
If a job fails, check the following:
- **Job Logs**: Available in AWS CloudWatch.
- **Data Catalog**: Ensure tables and metadata are correctly defined.
- **IAM Permissions**: Confirm that the Glue service has access to necessary resources.

## Contributing
Contributions are welcome! Please follow these steps:
1. Fork the repository.
2. Create a new branch (`git checkout -b feature-name`).
3. Commit your changes (`git commit -am 'Add feature'`).
4. Push the branch (`git push origin feature-name`).
5. Create a pull request.

## Contact
For questions, please reach out to the repository owner or open an issue.

---

**Lashmanbala**
