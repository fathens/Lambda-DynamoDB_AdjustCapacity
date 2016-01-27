from __future__ import print_function

import boto3
import json
import logging
import math
from datetime import datetime, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("Version 1.1.1")

cloudwatch = boto3.client('cloudwatch')

surplusRate = 1.2
upperThresholdRate = 0.8
lowerThresholdRate = 0.5

def lambda_handler(event, context):
    logger.info("Event: " + str(event))
    message = Message(event['Records'][0]['Sns']['Message'])
    logger.info("Message: " + str(message))

    metrics = Metrics(message)

    provision = metrics.getAvarage() * surplusRate

    def updateThroughput(src):
        map = {}
        for name in metricKeys.values():
            map[name] = src[name]

        map[metric.key] = provision
        return map

    table = boto3.resource('dynamodb').Table(message.getTableName())

    if message.getIndexName() == None:
        table.update(ProvisionedThroughput=updateThroughput(table.provisioned_throughput))
    else:
        index = next(iter(filter(lambda x: x['IndexName'] == message.getIndexName(), table.global_secondary_indexes)), None)
        if index == None:
            raise Exception('No index: ' + indexName)
        update = {
            'IndexName': indexName,
            'ProvisionedThroughput': updateThroughput(index['ProvisionedThroughput'])
        }
        table.update(GlobalSecondaryIndexUpdates=[{'Update': update}])

    Alarm(message.getUpperAlarmName()).updateThreshold(upperThresholdRate, provision)
    Alarm(message.getLowerAlarmName()).updateThreshold(lowerThresholdRate, provision)

class Message:
    def __init__(self, text):
        self.text = text
        self.src = json.loads(text)

    def __str__(self):
        return text

    def getMetricName(self):
        return self.src['Trigger']['MetricName']

    def getNamespace(self):
        return self.src['Trigger']['Namespace']

    def getDimensions(self):
        return self.src['Trigger']['Dimensions']

    def dimension(self, name):
        found = filter(lambda x: x['name'] == name, self.getDimensions())
        return next(iter(map(lambda x: x['value'], found)), None)

    def getTableName(self):
        return self.dimention('TableName')

    def getIndexName(self):
        return self.dimension('GlobalSecondaryIndexName')

    def getAlarmName(self):
        return self.src['AlarmName']

    def makeAlarmName(self, key):
        list = [self.getTableName(), self.getIndexName(), self.getMetricName(), key]
        return "-".join(filter(lambda x: x != None, list))

    def getUpperAlarmName(self):
        return makeAlarmName('Upper')

    def getLowerAlarmName(self):
        return makeAlarmName('Lower')

class Metrics:
    def __init__(self, message):
        self.message = message

        endTime = datetime.now()
        delta = timedelta(hours=1)
        startTime = endTime - delta

        self.statistics = cloudwatch.get_metric_statistics(
            Namespace=message.getNamespace(),
            MetricName=message.getMetricName(),
            Dimensions=message.getDimensions(),
            Statistics=['Average', 'Maximum'],
            StartTime=startTime,
            EndTime=endTime,
            Period=delta.seconds
        )

        self.key = {
            'ConsumedReadCapacityUnits': 'ReadCapacityUnits',
            'ConsumedWriteCapacityUnits': 'WriteCapacityUnits'
        }[message.getMetricName()]

    def getValue(self, key):
        return next(iter(map(lambda x: x[key], self.statistics['Datapoints'])), 1.0)

    def getAverage(self):
        return self.getValue('Average')

    def getMaximum(self):
        return self.getValue('Maximum')

class Alarm:
    def __init__(self, name):
        alarms = cloudwatch.describe_alarms(AlarmNames=[name])
        self.src = alarms['MetricAlarms'][0]

    def updateThreshold(self, rate, provision):
        period = self.src['Period']
        value = provision * rate * period

        cloudwatch.put_metric_alarm(
            AlarmName=self.src['AlarmName'],
            ActionsEnabled=self.src['ActionsEnabled'],
            MetricName=self.src['MetricName'],
            Namespace=self.src['Namespace'],
            Dimensions=self.src['Dimensions'],
            Statistic=self.src['Statistic'],
            OKActions=self.src['OKActions'],
            AlarmActions=self.src['AlarmActions'],
            InsufficientDataActions=self.src['InsufficientDataActions'],
            Period=period,
            EvaluationPeriods=self.src['EvaluationPeriods'],
            ComparisonOperator=self.src['ComparisonOperator'],
            Threshold=value
        )
