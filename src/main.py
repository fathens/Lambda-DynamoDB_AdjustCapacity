from __future__ import print_function

import sys
import json
import logging

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

    if namespace != cloudwatch.NAMESPACE:
        raise Exception("Namespace is not match: " + namespace)

    metric = cloudwatch.Metrics(dimensions, metricName)
    provision = metric.calcProvision()

    table = dynamodb.Table(metric.dimensions)
    table.update(metric.name, provision)

    for key in cloudwatch.BOUNDARIES.keys():
        metric.alarm(key).update(provision)

if __name__ == "__main__":
    logging.basicConfig()
    tableName = sys.argv[1]

    base = dynamodb.makeTable(tableName)
    indexes = map(lambda x: dynamodb.makeTable(tableName, x['IndexName']), base.getIndexes())

    for table in ([base] + indexes):
        for metricName in dynamodb.METRIC_KEYS.keys():
            metric = cloudwatch.Metrics(table.dimensions, metricName)
            for keyUL in cloudwatch.BOUNDARIES.keys():
                alarm = metric.alarm(keyUL)

                desc = alarm.describe()
                if desc == None:
                    logger.info("No alarm found, Creating...: " + alarm.getName())

                print("")
                alarm.update(metric.calcProvision())

                for key, value in alarm.describe().items():
                    logger.info("Alarm: " + key + " => " + str(value))
