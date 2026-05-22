import json
import boto3 
import sys
from datetime import datetime

streamname = 'tt-real-time-scd1-data-stream'
errorbucketname = 'tt-real-time-scd1-error-bucket'

# Initialize clients
s3_client = boto3.client('s3')
kinesis_client = boto3.client('kinesis')

def lambda_handler(event, context):
    try:
        # Assuming API Gateway passes JSON data in the event
        data = json.loads(event['body'])
        success_count = 0
        error_count = 0
        for record in data:          
            # print the records
            print(record)

            # Set the current timestamp
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")

            # Check if 'employee_id' column not exists or is blank    
            if 'employee_id' not in record or record['employee_id'] in [None, ""]:             
                # Include the timestamp in the object key
                object_key = f'error_{timestamp}.json'   

                # Write record to S3 bucket for error handling
                s3_client.put_object(Bucket=errorbucketname,Key=object_key,Body=json.dumps(record))
                error_count += 1
                       
            else:
                print('Writing to Amazon Kinesis stream')
                kinesis_client.put_record(StreamName=streamname, Data=json.dumps(record), PartitionKey='1' )
                success_count += 1
               
        response = {'statusCode': 200,'body': json.dumps({'message': 'Data Processing Completed','success_records': success_count,'error_records': error_count}),'headers': {'Content-Type': 'application/json'}}
        return response
           
    except Exception as e:
        print(f'Error processing event: {e}')
       
        error_response = {'statusCode': 500,'body': json.dumps(f'Error processing event: {e}'),'headers': {'Content-Type': 'application/json'}}
        return error_response
