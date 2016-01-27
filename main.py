from __future__ import print_function

import boto3
import json
import logging
import math

logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("Version 1.1.1")

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

        def updateAlarm(provision):
            cloudwatch = boto3.client('cloudwatch')
            alarms = cloudwatch.describe_alarms(AlarmNames=[message['AlarmName']])
            alarm = alarms['MetricAlarms'][0]

            threshold = provision.getThreshold(alarm['Threshold'])

            cloudwatch.put_metric_alarm(AlarmName=alarm['AlarmName'],
                                        ActionsEnabled=alarm['ActionsEnabled'],
                                        MetricName=alarm['MetricName'],
                                        Namespace=alarm['Namespace'],
                                        Dimensions=alarm['Dimensions'],
                                        Statistic=alarm['Statistic'],
                                        OKActions=alarm['OKActions'],
                                        AlarmActions=alarm['AlarmActions'],
                                        InsufficientDataActions=alarm['InsufficientDataActions'],
                                        Period=alarm['Period'],
                                        EvaluationPeriods=alarm['EvaluationPeriods'],
                                        ComparisonOperator=alarm['ComparisonOperator'],
                                        Threshold=threshold)

        provision = Provision()
        def updateThroughput(src):
            map = {}
            for name in metricKeys.values():
                map[name] = src[name]

            provision.preValue = map[metricKey]
            map[metricKey] = provision.getValue()
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

        updateAlarm(provision)

class Provision:
    def __init__(self):
        self.preValue = 0

    def getValue(self):
        return int(math.ceil(self.preValue * 1.2))

    def getThreshold(self, current):
        result = current * (float(self.getValue()) / self.preValue)
        logger.info("Change provision(%s -> %s), threshold(%s -> %s)" % (self.preValue, self.getValue(), current, result))
        return result
