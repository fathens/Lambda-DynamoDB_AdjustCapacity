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

        def enhance(src):
            provision = int(math.ceil(src * 1.2))

            alarm = boto3.resource('cloudwatch').Alarm(message['AlarmName'])
            boto3.client('cloudwatch').put_metric_alarm(AlarmName=alarm.name,
                                        Threshold=(dst * 0.8 * 60))

            return provision

        def updateThroughput(throughput):
            map = {}
            for name in metricKeys.values():
                map[name] = throughput[name]

            map[metricKey] = enhance(map[metricKey])
            return map

        if indexName == None:
            table.update(ProvisionedThroughput=updateThroughput(table.provisioned_throughput))
        else:
            index = next(iter(filter(lambda x: x['IndexName'] == indexName, table.global_secondary_indexes)), None)
            if index == None:
                raise Exception('No index: ' + indexName)
            update = {
                'IndexName': indexName,
                'ProvisionedThroughput': updateThroughput(index['ProvisionedThroughput'])
            }
            table.update(GlobalSecondaryIndexUpdates=[{'Update': update}])
