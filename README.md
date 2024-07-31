# NSE Data Processing with Airflow, Docker, and AWS

![Lambda code area](/Images/breakout_stock_flow_v2.png)


This guide provides detailed steps to set up and automate the data processing pipeline using Apache Airflow, Docker, and AWS services. Follow the instructions carefully to ensure a smooth setup.

## Steps to Follow

### 1. Setup NSE Data Dumper
- Configure the `nse_data_dumper.py` file.
- Check the `config.json` and `output_data/symbol_data.parquet` (if present) for required configurations and available data.

### 2. Adjust Docker Compose Volumes
- Edit the `docker-compose.yml` file to ensure volumes are correctly pointing to the scripts and data.

### 3. Update DAG Script
- Modify the DAG script to reference the correct location of your Python script within the Docker container.

### 4. Add Requirements (Optional)
- Add a `REQUIREMENT.TXT` to the Docker Compose file if needed. This approach is feasible for small projects.

### 5. Create `.env` File
- Create a `.env` file with the following content:

    ```plaintext
    AIRFLOW_IMAGE_NAME=apache/airflow:2.4.2
    AIRFLOW_UID=50000
    AWS_ACCESS_KEY_ID=
    AWS_SECRET_ACCESS_KEY=
    AWS_DEFAULT_REGION=ap-south-1
    ```

- Reference these variables in the Docker Compose file for environment setup.

### 6. Install Boto3
- Install `boto3` or add it to the requirements file for AWS interaction.

### 7. Add Spark Image to Docker Compose (optional)
- To use Spark instead of Athena, add the Spark image to your `docker-compose.yml`.
- Ensure AWS access keys are added to the environment variables.
- Create a network to allow communication between containers.

### 8. Troubleshoot S3 Data Read with PySpark
- If errors occur while reading data from S3 using PySpark, verify your AWS configuration.
- Consider adding necessary JAR files or editing `spark-defaults.conf`.
- Use the `s3a://` prefix as required by Hadoop AWS.

    ```python
    # Define the S3 path
    s3_path = "s3a://symboldatabucket86/symboldata/all_symbols_data.parquet"
    
    # Read data from S3
    df = spark.read.parquet(s3_path)
    ```

### 9. Using IAM Roles on EC2
- If deploying on EC2, leverage IAM roles for permissions and avoid hardcoding keys.

### Fixes and Customization
- Update `scripts/nse_data_dumper.py`, replacing `symbols[:5]` with `symbol` on line 182.

### 10. Athena Setup
- Set up Athena and configure the query output bucket.
- Create necessary databases and tables using the script `scripts/data_processing/athena_tablecreation.sql`.

### 11. Add Athena DAG
- Create an Athena DAG following a Python operator.
- Use `scripts/data_processing/athena_proccessing.sql` for the query.
- Set the query output bucket to `s3://athena-result-bucket-ankit86/codeoutput/`.

### 12. Create SNS Topic
- Create an SNS topic and subscribe emails for alerts. Don't forget the email verification step.

### 13. Create Lambda Function
- Create a Lambda function using the Python runtime.
- Copy the code from `scripts/data_processing/lambda_message.py` into the Lambda function editor.

    ![Lambda code area](/Images/lambda.png)

### 14. Add Environment Variable to Lambda
- Add `sns_topic_arn` environment variable with your SNS topic ARN.

    ![Lambda Env variable](/Images/envvar.png)

### 15. Attach Role to Lambda
- Attach a role to your Lambda function granting necessary permissions like S3 read and SNS publish.

### 16. Add S3 Event Notification
- Configure the S3 bucket (`s3://athena-result-bucket-ankit86/codeoutput/`) to trigger the Lambda function on new object creation events.

### 17. Test the Setup
- Upload a sample `.csv` file to the S3 bucket to test the entire flow.

    ![Breakout stock flow diagram](/Images/email.png)

Ensure all steps are followed correctly to set up a robust data processing pipeline. Happy Automating!
