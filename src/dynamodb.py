import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

METRIC_KEYS = {
    'ConsumedReadCapacityUnits': 'ReadCapacityUnits',
    'ConsumedWriteCapacityUnits': 'WriteCapacityUnits'
}

def makeDimensions(tableName, indexName):
    return [{'Name': 'TableName', 'Value': tableName},
            {'Name': 'GlobalSecondaryIndexName', 'Value': indexName}]

class Table:
    def __init__(self, dimensions):
        self.dimensions = dimensions
        def dim(key):
            found = filter(lambda x: x['Name'] == key, dimensions)
            return next(iter(map(lambda x: x['Value'], found)), None)
        self.tableName = dim('TableName')
        self.indexName = dim('GlobalSecondaryIndexName')
        self.src = boto3.resource('dynamodb').Table(self.tableName)

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
