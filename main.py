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
thresholdRate = {'Upper': 0.8, 'Lower': 0.5}

metricKeys = {
    'ConsumedReadCapacityUnits': 'ReadCapacityUnits',
    'ConsumedWriteCapacityUnits': 'WriteCapacityUnits'
}

def lambda_handler(event, context):
    logger.info("Event: " + str(event))
    message = Message(event['Records'][0]['Sns']['Message'])
    logger.info("Message: " + str(message))

    metrics = Metrics(message)

    provision = int(math.ceil(metrics.getAverage() * surplusRate))

    Table(message.getTableName(), message.getIndexName()).update(metrics.key, provision)

    for key, rate in thresholdRate:
        Alarm(message.makeAlarmName(key)).update(rate, provision)

class Message:
    def __init__(self, text):
        self.src = json.loads(text)

    def __str__(self):
        return json.dumps(self.src, indent=4)

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
        return self.dimension('TableName')

    def getIndexName(self):
        return self.dimension('GlobalSecondaryIndexName')

    def getAlarmName(self):
        return self.src['AlarmName']

    def makeAlarmName(self, key):
        list = [self.getTableName(), self.getIndexName(), self.getMetricName(), key]
        return "-".join(filter(lambda x: x != None, list))

class Metrics:
    def __init__(self, message):
        self.message = message

        endTime = datetime.now()
        delta = timedelta(hours=1)
        startTime = endTime - delta

        def fixDim(x):
            map = {}
            for key, value in x.items():
                map[key.capitalize()] = value
            return map

        self.statistics = cloudwatch.get_metric_statistics(
            Namespace=self.message.getNamespace(),
            MetricName=self.message.getMetricName(),
            Dimensions=map(fixDim, self.message.getDimensions()),
            Statistics=['Average', 'Maximum'],
            StartTime=startTime,
            EndTime=endTime,
            Period=delta.seconds
        )

        self.key = metricKeys[message.getMetricName()]

    def getValue(self, key):
        logger.info("Current Metrics: " + str(self.statistics))
        return next(iter(map(lambda x: x[key], self.statistics['Datapoints'])), 0.1)

    def getAverage(self):
        return self.getValue('Average')

    def getMaximum(self):
        return self.getValue('Maximum')

class Table:
    def __init__(self, tableName, indexName):
        self.tableName = tableName
        self.indexName = indexName
        self.src = boto3.resource('dynamodb').Table(tableName)

    def update(self, metricKey, provision):
        logger.info("Updating provision %s(%s) %s: %s" % (self.tableName, self.indexName, metricKey, provision))

        def updateThroughput(src):
            map = {}
            for name in metricKeys.values():
                map[name] = src[name]

            map[metricKey] = provision
            return map

        if self.indexName == None:
            self.src.update(ProvisionedThroughput=updateThroughput(self.src.provisioned_throughput))
        else:
            index = next(iter(filter(lambda x: x['IndexName'] == self.indexName, self.src.global_secondary_indexes)), None)
            if index == None:
                raise Exception('No index: ' + indexName)
            update = {
                'IndexName': indexName,
                'ProvisionedThroughput': updateThroughput(index['ProvisionedThroughput'])
            }
            self.src.update(GlobalSecondaryIndexUpdates=[{'Update': update}])


class Alarm:
    def __init__(self, name):
        self.name = name
        alarms = cloudwatch.describe_alarms(AlarmNames=[name])
        self.src = alarms['MetricAlarms'][0]

    def update(self, rate, provision):
        period = self.src['Period']
        value = provision * rate * period
        logger.info("Updating threshold %s: %s" % (self.name, value))

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
