from __future__ import print_function

import boto3
import json
import logging
import math

logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("Version 1.0.0")

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

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(dimension('TableName'))
        indexName = dimension('GlobalSecondaryIndexName')

        def enhance(src):
            dst = int(math.ceil(src * 1.2))
            logger.info("Enhance %s(%s) Throughput['%s']: %s => %s" % (table.name, indexName, metricKey, src, dst))
            return dst

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
