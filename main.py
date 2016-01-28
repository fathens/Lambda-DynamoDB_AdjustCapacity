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

SURPLUS_RATE = 1.2
THRESHOLD_RATE = {'Upper': 0.8, 'Lower': 0.5}

def lambda_handler(event, context):
    logger.info("Event: " + str(event))
    message = Message(event['Records'][0]['Sns']['Message'])
    logger.info("Message: " + str(message))

    def calc():
        RERIOD = timedelta(minutes=10)
        ave = Metrics(message).getAverage(RERIOD)
        if ave == None:
            ave = 0.1
        return int(math.ceil(ave * SURPLUS_RATE))

    def update(provision):
        table = Table(message.getTableName(), message.getIndexName())
        table.update(message.getMetricName(), provision)

        for key, rate in THRESHOLD_RATE.items():
            Alarm(message.makeAlarmName(key)).update(rate, provision)

    update(calc())

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
        return "-".join(filter(lambda x: x != None, list)).replace('.', '-')

class Metrics:
    def __init__(self, message):
        self.message = message
        def fixDim(x):
            map = {}
            for key, value in x.items():
                map[key.capitalize()] = value
            return map
        self.dimensions = map(fixDim, message.getDimensions())

    def getValue(self, key, period):
        endTime = datetime.now()
        startTime = endTime - period

        statistics = cloudwatch.get_metric_statistics(
            Namespace=self.message.getNamespace(),
            MetricName=self.message.getMetricName(),
            Dimensions=self.dimensions,
            Statistics=[key],
            StartTime=startTime,
            EndTime=endTime,
            Period=period.seconds
        )

        logger.info("Current Metrics: " + str(statistics))
        return next(iter(map(lambda x: x[key], statistics['Datapoints'])), None)

    def getAverage(self, period):
        return self.getValue('Average', period)

    def getMaximum(self, period):
        return self.getValue('Maximum', period)

class Table:
    def __init__(self, tableName, indexName):
        self.tableName = tableName
        self.indexName = indexName
        self.src = boto3.resource('dynamodb').Table(tableName)

    def update(self, metricName, provision):
        metricKeys = {
            'ConsumedReadCapacityUnits': 'ReadCapacityUnits',
            'ConsumedWriteCapacityUnits': 'WriteCapacityUnits'
        }
        metricKey = metricKeys[metricName]
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
        self.src = next(iter(alarms['MetricAlarms']), None)
        if self.src == None:
            raise Exception("No alarm found: " + name)

    def update(self, rate, provision):
        period = self.src['Period']
        value = provision * rate
        if value <= 0.5:
            value = 0
        threshold = value * period
        logger.info("Updating threshold %s: %s * %s = %s" % (self.name, value, period, threshold))

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
            Threshold=threshold
        )
