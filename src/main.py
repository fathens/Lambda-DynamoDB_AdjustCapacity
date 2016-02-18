from __future__ import print_function

import boto3
import json
import logging
import math
from datetime import datetime, timedelta

import dynamodb
import cloudwatch

logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("Version 2.0.1")

SURPLUS_RATE = 1.2
THRESHOLD_RATE = {'Upper': 0.8, 'Lower': 0.5}

def lambda_handler(event, context):
    logger.info("Event: " + str(event))
    message = Message(event['Records'][0]['Sns']['Message'])
    logger.info("Message: " + str(message))

    def calc():
        RERIOD = timedelta(minutes=10)
        ave = cloudwatch.Metrics(message.getNamespace(), message.getMetricName(), message.getDimensions()).getAverage(RERIOD)
        if ave == None:
            ave = 0.1
        return int(math.ceil(ave * SURPLUS_RATE))

    def update(provision):
        table = dynamodb.Table(message.getTableName(), message.getIndexName())
        table.update(message.getMetricName(), provision)

        for key, rate in THRESHOLD_RATE.items():
            cloudwatch.Alarm(message.makeAlarmName(key)).update(rate, provision)

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
