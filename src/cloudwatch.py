import boto3
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

cloudwatch = boto3.client('cloudwatch')

SURPLUS_RATE = 1.2

class Alarm:
    def __init__(self, name):
        self.name = name

    def create(self):
        return

    def update(self, rate, provision):
        alarms = cloudwatch.describe_alarms(AlarmNames=[name])
        alarm = next(iter(alarms['MetricAlarms']), None)
        if alarm == None:
            raise Exception("No alarm found: " + name)

        period = alarm['Period']
        value = provision * rate
        if value <= 0.5:
            value = 0
        threshold = value * period
        logger.info("Updating threshold %s: %s * %s = %s" % (self.name, value, period, threshold))

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
