Table of Contents

Overview
Architecture
Tech Stack
Pipeline Walkthrough

1. Data Ingestion
2. Data Processing
3. Data Streaming & Storage
4. Snowflake Integration
5. Raw Layer
6. Transformed Layer


Key Concepts


Overview
This project demonstrates an end-to-end real-time streaming pipeline built on AWS and Snowflake. Data is simulated via Postman, routed through a serverless ingestion and validation layer, streamed into S3, and automatically loaded into Snowflake — where it is cleaned, deduplicated, and kept current using SCD Type 1 logic via Streams and Tasks.

Architecture

<img width="1600" height="411" alt="image" src="https://github.com/user-attachments/assets/bce5c60d-0751-4581-bcd7-0059e8608ada" />



Tech Stack
LayerService / ToolIngestionPostman, AWS API GatewayProcessingAWS LambdaStreamingAmazon Kinesis Data StreamsDeliveryAmazon Kinesis Data FirehoseStorageAmazon S3EventingAmazon SQSData WarehouseSnowflake (Snowpipe, Streams, Tasks)

Pipeline Walkthrough
1. Data Ingestion
HTTP POST requests are sent via Postman to AWS API Gateway, which acts as the entry point for all incoming data.
2. Data Processing
API Gateway routes requests to an AWS Lambda function that performs validation and transformation:

Valid records → forwarded to Amazon Kinesis Data Streams
Invalid / malformed records → written to a dedicated S3 error bucket for troubleshooting and reprocessing

3. Data Streaming & Storage
Amazon Kinesis Data Streams captures the real-time flow of validated data. Kinesis Data Firehose consumes that stream and delivers records to an S3 data bucket in structured JSON format, enabling durable and scalable storage.
4. Snowflake Integration
An external stage in Snowflake points to the S3 data bucket. S3 event notifications trigger an SQS queue, which in turn triggers Snowpipe to automatically load new data into Snowflake raw tables in near real-time.
5. Raw Layer
Data lands in raw tables as-is — no transformation applied at this stage.
6. Transformed Layer
Data from the raw layer is cleaned, deduplicated, and processed through SCD Type 1 logic:

Existing records are overwritten with the latest values (no history retained)
Snowflake Streams track new or changed records so only deltas are processed
Snowflake Tasks orchestrate the transformation automatically on a schedule


Key Concepts
Real-Time Streaming — the continuous flow and processing of data as it is generated, enabling immediate insights and actions without delay.
SCD Type 1 (Slowly Changing Dimension Type 1) — a data warehousing pattern where old values are simply overwritten with new ones, without retaining any historical record of prior states.
