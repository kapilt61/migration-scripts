import os


class MongoConfig:
    @staticmethod
    def get():

        return {
            'env': "local",
            'host': os.getenv('MONGO_HOST', 'localhost:27019'),
            'user': os.getenv('MONGO_USER', 'admin'),
            'password': os.getenv('MONGO_PASSWORD', 'admin123'),
            'dbname': os.getenv('MONGO_DB_NAME', 'slot_planner_au'),
            'collection': os.getenv('MONGO_COLLECTION', 'roster'),
            'dumpFile': os.getenv('ARANGO_DUMP', 'roster-slot-planner-au-stage.json'),
            'mappingCollection': os.getenv('MONGO_MAPPING_COLLECTION', 'vehicle_log'),
            'mappingDumpFile': os.getenv('ARANGO_MAPPING_DUMP',
                                         'results-b2c_sourcing_service_au_stage_vehicle_log.json'),
            'mappingKey': 'vehicle',
            'mappingKeyType': 'object',
            "mappingKeyId": "_id",  ## set when mappingKeyType is object
            "extraInternalMappingId": "tradeId",
            'versionReq': os.getenv('VERSION_REQ', True),
            "mapArangoFieldToMongo": False,
            "mongoField": "_id",  ## required mapArangoFieldToMongoId is true
            "arangoFieldType": "string",
            "arangoField": "_key"  ## required mapArangoFieldToMongoId is true
    }
