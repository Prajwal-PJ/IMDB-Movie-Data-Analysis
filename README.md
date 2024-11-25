# IMDB Movie Data End-to-End Pipeline
An end-to-end pipeline for processing IMDB movie data using AWS services.

# Project Overview
Project Overview
This project demonstrates an end-to-end data pipeline built on AWS for processing and analyzing IMDB movie data. The pipeline is designed to automate data ingestion, quality checks, transformation, and loading into a Redshift data warehouse for further analysis. Notifications are sent at various stages of the pipeline for monitoring and alerting.

# Architecture
* The architecture leverages the following AWS services:<br/>
* S3: For storing raw, intermediate, and processed data.<br/>
* AWS Glue: For building the data catalog, running ETL jobs, and implementing data quality checks.<br/>
* AWS Redshift: As the final destination for processed data.<br/>
* Athena & Quicksight: For querying and visualizing failed transformation records.<br/>
* Amazon EventBridge: For scheduling and monitoring ETL jobs.<br/>
* Amazon SNS: For sending success and failure notifications.<br/>



# Pipeline Workflow
![image](https://github.com/user-attachments/assets/c81ae5a9-16bb-40a8-91f8-39e0f2ac622f)<br/>

* Load IMDB movie data into S3.<br/> ![image](https://github.com/user-attachments/assets/5e0ef8b0-c83f-4023-878a-5ee860647044)<br/>

* Use AWS Glue to create a Data Catalog Crawler for the data in S3.<br/> ![image](https://github.com/user-attachments/assets/449b6e0d-4482-4b14-8750-75827ff00f37)<br/> ![image](https://github.com/user-attachments/assets/bd66f62d-18b1-401b-b142-1018d5bc3c0a)<br/>


* Apply Glue Data Quality Rules to validate the source data.<br/>![image](https://github.com/user-attachments/assets/0e0ce149-0a2d-4d09-88a1-6e3c7947fafc)<br/>

* Outcomes of the rules are written to another S3 bucket for analysis.<br/>![image](https://github.com/user-attachments/assets/d6a9a315-fa8c-4abb-8317-77f7d59e8014)<br/>

* Set up a Redshift cluster as the destination for the processed data.<br/>![image](https://github.com/user-attachments/assets/1a3e918d-95d5-4c70-9bef-53cf88476402)<br/>
* Create a Schema for the Destination table.

* Create a Glue Crawler to catalog the Redshift destination table.<br/>![image](https://github.com/user-attachments/assets/42bb22c2-384f-4e33-99ab-18acce000b59)<br/>![image](https://github.com/user-attachments/assets/16207fd4-84cd-4399-b01a-4251f594d9c6)


* Implement an ETL workflow in AWS Glue:<br/>
* Transform the data:<br/>![image](https://github.com/user-attachments/assets/d3fbd9e6-8d82-4969-808f-9ff8305433df) <br/>

* Route failed records to an S3 bucket for further analysis using Athena and Quicksight.<br/>![image](https://github.com/user-attachments/assets/644dc70d-be31-4d51-b906-897b74edc85e)<br/>

* Route passed records to the Redshift destination.<br/>![image](https://github.com/user-attachments/assets/36306644-9b17-49a3-9d4a-f47604acb8f3)<br/>

* Configure Amazon EventBridge and SNS for notifications on ETL success or failure.<br/>

# Tech Stack
* AWS Services: S3, Glue, Redshift, Athena, Quicksight, EventBridge, SNS<br/>
* Programming Languages: Python (for Glue scripts and Lambda functions)<br/>
* Frameworks/Tools: Boto3, Pandas, SQL<br/>
* Visualization: AWS Quicksight<br/>

# Features
* Data Ingestion: Automatically loads raw IMDB data into S3.<br/>
* Data Quality Validation: Implements Glue Data Quality Rules to ensure data consistency.<br/>
* Data Transformation: Processes and routes records based on validation outcomes.<br/>
* Data Storage:<br/>
* Passed records → Redshift for analytics.<br/>
* Failed records → S3 for further review via Athena and Quicksight.<br/>
* Notifications: Uses EventBridge and SNS for alerts on ETL job status.<br/>

# Getting Started<br/>
* AWS Account with appropriate permissions.<br/>
* Python environment with required dependencies (boto3, pandas).<br/>
* IMDB movie dataset (sample files provided in the data/ folder).<br/>
