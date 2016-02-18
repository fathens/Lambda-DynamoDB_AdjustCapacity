from __future__ import print_function

import json
import logging
import math
from datetime import timedelta

import dynamodb
import cloudwatch

logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("Version 2.0.1")

def lambda_handler(event, context):
    logger.info("Event: " + str(event))
    trigger = json.loads(event['Records'][0]['Sns']['Message']['Trigger'])
    logger.info("Trigger: " + json.dumps(trigger, indent=4))

    metricName = trigger['MetricName']
    namespace = trigger['Namespace']
    dimensions = trigger['Dimensions']

    def calc():
        RERIOD = timedelta(minutes=10)
        ave = cloudwatch.Metrics(namespace, metricName, dimensions).getAverage(RERIOD)
        if ave == None:
            ave = 0.1
        return int(math.ceil(ave * cloudwatch.SURPLUS_RATE))

    def update(provision):
        table = dynamodb.Table(dimensions)
        table.update(metricName, provision)

        for key, rate in cloudwatch.THRESHOLD_RATE.items():
            cloudwatch.Alarm(table.dimensions, metricName, key).update(rate, provision)

    update(calc())
