import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

METRIC_KEYS = {
    'ConsumedReadCapacityUnits': 'ReadCapacityUnits',
    'ConsumedWriteCapacityUnits': 'WriteCapacityUnits'
}

THRESHOLD_RATE = {'Upper': 0.8, 'Lower': 0.5}

class Table:
    def __init__(self, tableName, indexName):
        self.tableName = tableName
        self.indexName = indexName
        self.src = boto3.resource('dynamodb').Table(tableName)

    def makeAlarmName(self, metricName, key):
        list = [self.tableName, self.indexName, metricName, key]
        return "-".join(filter(lambda x: x != None, list)).replace('.', '-')

    def update(self, metricName, provision):
        metricKey = METRIC_KEYS[metricName]
        logger.info("Updating provision %s(%s) %s: %s" % (self.tableName, self.indexName, metricKey, provision))

        def updateThroughput(src):
            map = {}
            for name in METRIC_KEYS.values():
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
