from __future__ import print_function

import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info("Event: " + str(event))
    message = json.loads(event['Records'][0]['Sns']['Message'])
    logger.info("Message: " + str(message))

    if message['NewStateValue'] != 'ALARM':
        context.succeed('NotAlarm')
    else:
        dynamodb = boto3.resource('dynamodb')

        def metricKey():
            metric = message['Trigger']['MetricName']
            if metric == 'ConsumedReadCapacityUnits':
                return 'ReadCapacityUnits'
            elif metric == 'ConsumedWriteCapacityUnits':
                return 'WriteCapacityUnits'

        def dimension(name):
            list = message['Trigger']['Dimensions']
            return next(map(lambda x: x['value'], filter(lambda x: x['name'] == name, list)), None)

        def updateThroughput(throuput):
            key = metricKey()
            throuput[key] = throuput[key] * 1.2
            return throuput

        table = dynamodb.Table(dimension('TableName'))
        indexName = dimension('GlobalSecondaryIndexName')
        if indexName == None:
            table.update(ProvisionedThroughput=updateThroughput(table.provisioned_throughput))
        else:
            index = next(filter(lambda x: x['IndexName'] == indexName, table.global_secondary_indexes), None)
            if index == None:
                context.fail('No index: ' + indexName);
            else:
                table.update(GlobalSecondaryIndexUpdates=[{'Update': {
                                                                      'IndexName': indexName,
                                                                      'ProvisionedThroughput': updateThroughput(index['ProvisionedThroughput'])
                                                                      }
                                                           }])
        context.succeed('OK')
