import boto3
import logging
import math
from datetime import datetime, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

cloudwatch = boto3.client('cloudwatch')

NAMESPACE = 'AWS/DynamoDB'

SURPLUS_RATE = 1.2
BOUNDARIES = {
    'Upper': {
        'Period': 900,
        'Threshold': 0.8,
        'ComparisonOperator': 'GreaterThanOrEqualToThreshold',
        'SNSKey': 'enhance'
    },
    'Lower': {
        'Period': 3600,
        'Threshold': 0.5,
        'ComparisonOperator': 'LessThanThreshold',
        'SNSKey': 'reduce'
    }
}
SNS_PREFIX = 'arn:aws:sns:us-east-1:226562473592:dynamodb-capacity-'

METRIC_RERIOD = timedelta(minutes=10)

class Alarm:
    def __init__(self, metric, keyUL):
        self.metric = metric
        self.keyUL = keyUL

    def getName(self):
        list = map(lambda x: x['Value'], self.metric.dimensions) + [self.metric.name, self.keyUL]
        return "-".join(list).replace('.', '-').replace('_', '-')

    def getSNSName(self):
        tableName = filter(lambda x: x['Name'] == 'TableName', self.metric.dimensions)[0]['Value']
        name = SNS_PREFIX + BOUNDARIES[self.keyUL]['SNSKey']
        if tableName.split('.')[0].endswith('-TEST'):
            name = name + '-test'
        return name

    def describe(self):
        names = [self.getName()]
        logger.info("Finding alarm: " + str(names))
        alarms = cloudwatch.describe_alarms(AlarmNames=names)
        return next(iter(alarms['MetricAlarms']), None)

    def update(self, provision):
        boundary = BOUNDARIES[self.keyUL]
        alarm = self.describe()
        if alarm == None:
            logger.info("No alarm found, Creating...: " + self.getName())

        period = boundary['Period']
        value = provision * boundary['Threshold']
        if value <= 0.5:
            value = 0
        threshold = value * period
        logger.info("Updating threshold %s: %s * %s = %s" % (self.getName(), value, period, threshold))

        cloudwatch.put_metric_alarm(
            AlarmName=self.getName(),
            ActionsEnabled=True,
            MetricName=self.metric.name,
            Namespace=NAMESPACE,
            Dimensions=self.metric.dimensions,
            Statistic='Sum',
            OKActions=[],
            AlarmActions=[self.getSNSName()],
            InsufficientDataActions=[],
            Period=period,
            EvaluationPeriods=1,
            ComparisonOperator=boundary['ComparisonOperator'],
            Threshold=threshold
        )

class Metrics:
    def __init__(self, dimensions, metricName):
        self.name = metricName
        def fixDim(x):
            map = {}
            for key, value in x.items():
                map[key.capitalize()] = value
            return map if map['Value'] != None else None
        self.dimensions = filter(lambda x: x != None, map(fixDim, dimensions))

    def alarm(self, keyUL):
        return Alarm(self, keyUL)

    def getValue(self, key, period):
        endTime = datetime.now()
        startTime = endTime - period * 2

        statistics = cloudwatch.get_metric_statistics(
            Namespace=NAMESPACE,
            MetricName=self.name,
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

    def calcProvision(self):
        ave = self.getAverage(METRIC_RERIOD)
        if ave == None:
            ave = 0.1
        return int(math.ceil(ave * SURPLUS_RATE))
