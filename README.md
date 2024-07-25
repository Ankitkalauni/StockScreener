# Stock Breakout Alert
Stack: Airflow, Python, Athena or Spark, Yahoo Finance API, SNS

## Steps to Follow

### 1. Setup NSE Data Dumper
- Configure the `nse_data_dumper.py` file.
- Check the `config.json` and `output_data/symbol_data.parquet` (if present).

### 2. Adjust Docker Compose Volumes
- Change or fix the volume of the `docker-compose.yml` file to point to the correct location of the scripts and data.

### 3. Update DAG Script
- Modify the DAG script to point to the proper location of the Python script. Note that this location should be the Docker container's path, not the local path.

### 4. Add Requirements (Optional)
- Add `REQUIREMENT.TXT` to the Docker Compose file. This is not recommended for large projects, but it works for small projects.

### 5. Create `.env` File
- Create a `.env` file and add the following content:

    ```plaintext
    AIRFLOW_IMAGE_NAME=apache/airflow:2.4.2
    AIRFLOW_UID=50000
    AWS_ACCESS_KEY_ID=
    AWS_SECRET_ACCESS_KEY=
    AWS_DEFAULT_REGION=ap-south-1
    ```

- Add these variables to the Docker Compose file.

### 6. Install Boto3
- Install `boto3` or add it to the requirements file.

### 7. Add Spark Image to Docker Compose
- Add the Spark image to the `docker-compose.yml` file.
- Add the AWS access key to the environment variables.
- Create a network to allow interaction between containers.

### 8. Troubleshoot S3 Data Read with PySpark
- If you encounter errors while reading data from S3 using PySpark, check the AWS configuration.
- You may need to add a JAR file or edit the `spark-defaults.conf`.
- Use `s3a://` prefix as Hadoop AWS uses this.

    ```python
    # Define the S3 path
    s3_path = "s3a://symboldatabucket86/symboldata/all_symbols_data.parquet"
    
    # Read data from S3
    df = spark.read.parquet(s3_path)
    ```

### 9. Using IAM Roles on EC2
- If you are using an EC2 instance, you can leverage IAM roles.

### Fixes and Customization
- Replace `symbols[:5]` with `symbol` on line {182} in `scripts/nse_data_dumper.py`.
