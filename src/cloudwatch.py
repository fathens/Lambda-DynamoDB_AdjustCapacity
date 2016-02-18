import boto3
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

cloudwatch = boto3.client('cloudwatch')

SURPLUS_RATE = 1.2
THRESHOLD_RATE = {'Upper': 0.8, 'Lower': 0.5}

class Alarm:
    def __init__(self, dimensions, metricName, keyUL):
        self.dimensions = dimensions
        self.metricName = metricName
        self.keyUL = keyUL

    def getName(self):
        list = map(lambda x: x['Value'], self.dimensions) + [self.metricName, self.keyUL]
        return "-".join(filter(lambda x: x != None, list)).replace('.', '-')

    def create(self, namespace, comparison, action):
        period = 900
        threshold = 720.0

        cloudwatch.put_metric_alarm(
            AlarmName=self.getName(),
            ActionsEnabled=True,
            MetricName=self.metricName,
            Namespace=namespace,
            Dimensions=self.dimensions,
            Statistic='Sum',
            OKActions=[],
            AlarmActions=[action],
            InsufficientDataActions=[],
            Period=period,
            EvaluationPeriods=1,
            ComparisonOperator=comparison,
            Threshold=threshold
        )

    def describe(self):
        names = [self.getName()]
        logger.info("Finding alarm: " + str(names))
        alarms = cloudwatch.describe_alarms(AlarmNames=names)
        return next(iter(alarms['MetricAlarms']), None)

    def update(self, rate, provision):
        alarm = self.describe()
        if alarm == None:
            raise Exception("No alarm found: " + self.getName())

        period = alarm['Period']
        value = provision * rate
        if value <= 0.5:
            value = 0
        threshold = value * period
        logger.info("Updating threshold %s: %s * %s = %s" % (self.getName(), value, period, threshold))

        cloudwatch.put_metric_alarm(
            AlarmName=alarm['AlarmName'],
            ActionsEnabled=alarm['ActionsEnabled'],
            MetricName=alarm['MetricName'],
            Namespace=alarm['Namespace'],
            Dimensions=alarm['Dimensions'],
            Statistic=alarm['Statistic'],
            OKActions=alarm['OKActions'],
            AlarmActions=alarm['AlarmActions'],
            InsufficientDataActions=alarm['InsufficientDataActions'],
            Period=period,
            EvaluationPeriods=alarm['EvaluationPeriods'],
            ComparisonOperator=alarm['ComparisonOperator'],
            Threshold=threshold
        )

class Metrics:
    def __init__(self, namespace, name, dimensions):
        self.namespace = namespace
        self.name = name
        def fixDim(x):
            map = {}
            for key, value in x.items():
                map[key.capitalize()] = value
            return map
        self.dimensions = map(fixDim, dimensions)

    def getValue(self, key, period):
        endTime = datetime.now()
        startTime = endTime - period * 2

        statistics = cloudwatch.get_metric_statistics(
            Namespace=self.namespace,
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
