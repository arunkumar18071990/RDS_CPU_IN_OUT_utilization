import boto3
import pandas as pd
from datetime import datetime, timedelta

# Initialize AWS CloudWatch client
cloudwatch = boto3.client('cloudwatch')

# Fetch list of RDS instances
rds = boto3.client('rds')
response = rds.describe_db_instances()
instances = [instance['DBInstanceIdentifier'] for instance in response['DBInstances']]

# List of metric names to fetch
metric_names = ['CPUUtilization', 'DatabaseConnections', 'ReadIOPS', 'WriteIOPS']

# Fetch and process RDS utilization metrics for each instance
data = []

for instance_id in instances:
    instance_data = [instance_id]

    for metric_name in metric_names:
        metric_query = {
            'Id': 'm1',
            'MetricStat': {
                'Metric': {
                    'Namespace': 'AWS/RDS',
                    'MetricName': metric_name,
                    'Dimensions': [{'Name': 'DBInstanceIdentifier', 'Value': instance_id}]
                },
                'Period': 3600,
                'Stat': 'Average'
            },
            'ReturnData': True
        }

        response = cloudwatch.get_metric_data(MetricDataQueries=[metric_query], StartTime=(), EndTime=datetime.now())

        if 'MetricDataResults' in response and len(response['MetricDataResults'][0]['Values']) > 0:
            values = response['MetricDataResults'][0]['Values']
            average_value = sum(values) / len(values)
            min_value = min(values)
            max_value = max(values)
            instance_data.extend([average_value, min_value, max_value])

    data.append(instance_data)

# Create a DataFrame from the fetched data
columns = ['Instance ID'] + [f'{metric} Avg' for metric in metric_names] + [f'{metric} Min' for metric in metric_names] + [f'{metric} Max' for metric in metric_names]
df = pd.DataFrame(data, columns=columns)

# Save the DataFrame to an Excel file
excel_file = 'rds_utilization_data.xlsx'
df.to_excel(excel_file, index=False, engine='openpyxl')

print(f"RDS utilization data saved to {excel_file}")
