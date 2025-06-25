import os


class MongoConfig:
    @staticmethod
    def get():
        return {
            'host': os.getenv('MONGO_HOST', 'localhost:27107'),
            'user': os.getenv('MONGO_USER', 'user'),
            'password': os.getenv('MONGO_PASSWORD', 'passwd'),
            'dbname': os.getenv('MONGO_DB_NAME', 'vehicle_onboarding_service_au_stage'),
            'collection': os.getenv('MONGO_COLLECTION', 'vehicle'),
            'dumpFile': os.getenv('ARANGO_DUMP', 'vehicle-vehicle_onboarding_service_au_stage.json'),
            'mappingCollection': os.getenv('MONGO_MAPPING_COLLECTION', 'vehicle_log'),
            'mappingDumpFile': os.getenv('ARANGO_MAPPING_DUMP',
                                         'vehicle_log-vehicle_onboarding_service_au_stage.json'),
            'mappingKey': 'vehicleId',
            'mappingKeyType': 'string',
            "mappingKeyId": "_id", ## set when mappingKeyType is object
            'versionReq': os.getenv('VERSION_REQ', True),
            "mapArangoFieldToMongo": True,
            "mongoField": "leadId", ## required mapArangoFieldToMongoId is true
            "arangoFieldType": "string",
            "arangoField": "_key"  ## required mapArangoFieldToMongoId is true
        }

    # @staticmethod
    # def get():
    #     return {
    #         'host': os.getenv('MONGO_HOST', 'localhost:27019'),
    #         'user': os.getenv('MONGO_USER', 'admin'),
    #         'password': os.getenv('MONGO_PASSWORD', 'admin123'),
    #         'dbname': os.getenv('MONGO_DB_NAME', 'vehicle'),
    #         'collection': os.getenv('MONGO_COLLECTION', 'vehicle_map'),
    #         'dumpFile': os.getenv('ARANGO_DUMP', 'results-b2c_vehicle_feed_service_au_stage.json'),
    #         'mappingCollection': os.getenv('MONGO_MAPPING_COLLECTION', 'vehicle_map_log'),
    #         'mappingDumpFile': os.getenv('ARANGO_MAPPING_DUMP',
    #                                      'results-b2c_vehicle_feed_service_au_stage (1).json'),
    #         'mappingId': 'vehicleMappingId',
    #         'versionReq': os.getenv('VERSION_REQ', False),
    #         "mapArangoFieldToMongoId": False,
    #         "arangoField": "_key"  ## required mapArangoFieldToMongoId is true
    #     }
