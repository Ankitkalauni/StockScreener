import boto3
import csv
import os
from datetime import datetime
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')


def format_percentage(value):
    return f"{value:.2f}%"


def construct_message(data):
    message = ""
    for item in data:
        
        message += f"""
        {item['symbol']}:
        
        Breakout Count: {item['breakout_cnt']}
        
        Max Price Difference: {format_percentage(item['max_price_diff'])}
        
        Volume Increase from Median Volume: {item['max_volume_diff']:.2f}%
        
        Max Breakout Price: {item['max_breakout_price']}
        
        Max Breakout Volume: {item['max_breakout_volume']:,}
        
        Min Median Max Price: {item['min_price']} | {item['median_price']} | {item['max_price']}
                
        \n"""
        
    return message.strip()

def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']


    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    content = response['Body'].read().decode('utf-8-sig').splitlines()

    csv_reader = csv.DictReader(content)
    

    data = []
    for row in csv_reader:
        data.append({
            'symbol': row['symbol'],
            'breakout_cnt': int(row['breakout_cnt']),
            'max_price_diff': float(row['max_price_diff']),
            'max_breakout_volume': int(row['max_breakout_volume']),
            'max_breakout_price': float(row['max_breakout_price']),
            'median_price': float(row['median_price']),
            'max_volume_diff': float(row['max_volume_diff']),
            'max_price': float(row['max_price']),
            'min_price': float(row['min_price']),
        })

    message = construct_message(data)

    sns_client.publish(
        TopicArn=sns_topic_arn,
        Subject=f"Stock Breakout Analysis: {datetime.now().date()}",
        Message=message
    )

    return {
        'statusCode': 200,
        'body': 'Notification sent successfully!'
    }