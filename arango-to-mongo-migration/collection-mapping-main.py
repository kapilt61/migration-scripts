import json
from datetime import datetime
from decimal import Decimal

import dateutil.parser
from pymongo import MongoClient

from config.mongo_config import MongoConfig as mongo_config

# === CONFIGURATION ===
MONGO_CONFIG = mongo_config.get()
ARANGO_DUMP_FILE = MONGO_CONFIG['dumpFile']
ARANGO_MAPPING_DUMP_FILE = MONGO_CONFIG['mappingDumpFile']
MONGO_URI = f"mongodb+srv://{MONGO_CONFIG['user']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}/?retryWrites=true&w=majority"
MONGO_DB = MONGO_CONFIG['dbname']
MONGO_COLLECTION = MONGO_CONFIG['collection']
MONGO_MAPPING_COLLECTION = MONGO_CONFIG['mappingCollection']
BATCH_SIZE = 100
MAX_WORKERS = 4
DATE_FIELDS = ['createdAt', 'updatedAt', 'timestamp', 'createdDt', 'modifiedDt', 'expireAt', 'quoteDate', 'createdDate', 'lastModifiedDate']  # Adjust as needed
VERSION_REQ = MONGO_CONFIG['versionReq']

# === SETUP ===
client = MongoClient(MONGO_URI, tls=True, tlsallowinvalidcertificates=True)
collection = client[MONGO_DB][MONGO_COLLECTION]


# === UTILITIES ===
def convert_arango_date(value):
    if isinstance(value, str):
        clean_value = value.split('[')[0]

        if len(clean_value) < 12 or clean_value.isdigit():
            return value

        try:
            parsed = dateutil.parser.isoparse(clean_value)
            return parsed
        except Exception:
            return value  # Not a valid date string
    elif isinstance(value, int) and len(str(value)) >= 12:
        # Heuristic: likely a millisecond timestamp
        try:
            return datetime.utcfromtimestamp(value / 1000.0)
        except Exception:
            return value
    return value

def process_dates(obj):
    if isinstance(obj, dict):
        return {k: process_dates(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [process_dates(item) for item in obj]
    else:
        return convert_arango_date(obj)
# def process_dates(doc):
#     for field in DATE_FIELDS:
#         if field in doc:
#             doc[field] = convert_arango_date(doc[field])
#     return doc

def convert_decimals(obj):
    if isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj


MONGO_FIELD = MONGO_CONFIG['mongoField']
ARANGO_FIELD = MONGO_CONFIG['arangoField']
TYPE = MONGO_CONFIG['arangoFieldType']
def clean_and_prepare(doc, mapping=True):
    doc.pop('_id', None)  # Let Mongo auto-generate _id
    if mapping and MONGO_CONFIG['mapArangoFieldToMongo'] is True:
        value = doc[ARANGO_FIELD]
        if TYPE == 'string':
            value = str(value)
        elif TYPE == 'int':
            value = int(value)
        elif TYPE == 'float':
            value = float(value)
        elif TYPE == 'bool':
            value = bool(value)
        doc[MONGO_FIELD] = value
    doc.pop('_rev', None)
    doc.pop('_from', None)
    doc.pop('_to', None)
    doc.pop('_key', None)
    if VERSION_REQ:
        doc['version'] = 0
    doc = convert_decimals(doc)
    return process_dates(doc)


id_map = {}

with open(ARANGO_DUMP_FILE) as f:
    data = json.load(f)

with open(ARANGO_MAPPING_DUMP_FILE) as f:
    data_logs = json.load(f)

mappingCollection = client[MONGO_DB][MONGO_MAPPING_COLLECTION]

# 1. Prepare vehicle docs (remove Arango keys)
mongo_docs = []
old_to_data = {}
for v in data:
    old_id = v.get('_key', None) or v.get('id')
    v = clean_and_prepare(v)
    v.pop('_id', None)
    old_to_data[old_id] = v
    mongo_docs.append(v)

# 2. Bulk insert vehicles
result = collection.insert_many(mongo_docs)

# 3. Map old_id -> new MongoDB _id
id_map = {}
for old_id, new_id in zip(old_to_data.keys(), result.inserted_ids):
    id_map[old_id] = new_id

# 4. Prepare updated vehicle_log docs
data_log_docs = []
mappingKeyType = MONGO_CONFIG['mappingKeyType']
mappingKey = MONGO_CONFIG['mappingKey']
for log in data_logs:

    if mappingKeyType == 'object':
        old_data = log.get(mappingKey)
        old_data_id = old_data['_key']
        old_data = clean_and_prepare(old_data, False)
        mappingKeyId = MONGO_CONFIG['mappingKeyId']
        if old_data_id in id_map:
            old_data[mappingKeyId] = id_map[old_data_id]
            log[mappingKey] = old_data
            log = clean_and_prepare(log, False)
            data_log_docs.append(log)

    else:
        old_vehicle_id = log.get(mappingKey)
        log = clean_and_prepare(log)
        if old_vehicle_id in id_map:
            log[mappingKey] = str(id_map[old_vehicle_id])
            data_log_docs.append(log)
        else:
            print(f"Skipping log with unknown vehicle_id: {old_vehicle_id}")

# 5. Bulk insert vehicle_logs
if data_log_docs:
    mappingCollection.insert_many(data_log_docs)
