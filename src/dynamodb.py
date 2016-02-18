import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
