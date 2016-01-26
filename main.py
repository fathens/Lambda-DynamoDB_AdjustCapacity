from __future__ import print_function

import boto3
import json
import logging
import math

logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("Version 1.0.1")

def lambda_handler(event, context):
    logger.info("Event: " + str(event))
    message = json.loads(event['Records'][0]['Sns']['Message'])
    logger.info("Message: " + str(message))

    if message['NewStateValue'] == 'ALARM':
        metricKeys = {
            'ConsumedReadCapacityUnits': 'ReadCapacityUnits',
            'ConsumedWriteCapacityUnits': 'WriteCapacityUnits'
        }
        metricKey = metricKeys[message['Trigger']['MetricName']]

        def dimension(name):
            list = message['Trigger']['Dimensions']
            return next(iter(map(lambda x: x['value'], filter(lambda x: x['name'] == name, list))), None)

        table = boto3.resource('dynamodb').Table(dimension('TableName'))
        indexName = dimension('GlobalSecondaryIndexName')

        throughput = {}
        def updateThroughput(src):
            for name in metricKeys.values():
                throughput[name] = src[name]

            throughput[metricKey] = int(math.ceil(throughput[metricKey] * 1.2))
            return map

        if indexName == None:
            updateThroughput(table.provisioned_throughput)
            table.update(ProvisionedThroughput=throughput)
        else:
            index = next(iter(filter(lambda x: x['IndexName'] == indexName, table.global_secondary_indexes)), None)
            if index == None:
                raise Exception('No index: ' + indexName)
            updateThroughput(index['ProvisionedThroughput'])
            update = {
                'IndexName': indexName,
                'ProvisionedThroughput': throughput
            }
            table.update(GlobalSecondaryIndexUpdates=[{'Update': update}])

        cloudwatch = boto3.client('cloudwatch')
        alarms = cloudwatch.describe_alarms(AlarmNames=[message['AlarmName']])
        logger.info("List of alarms: " + str(alarms))
        alarm = alarms['MetricAlarms'][0]
        cloudwatch.put_metric_alarm(AlarmName=alarm['AlarmName'],
                                    MetricName=alarm['MetricName'],
                                    Namespace=alarm['Namespace'],
                                    Statistic=alarm['Statistic'],
                                    Period=alarm['Period'],
                                    EvaluationPeriods=alarm['EvaluationPeriods'],
                                    ComparisonOperator=alarm['ComparisonOperator'],
                                    Threshold=(throughput[metricKey] * 0.8 * 60))
