import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

cloudwatch = boto3.client('cloudwatch')

SURPLUS_RATE = 1.2

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
