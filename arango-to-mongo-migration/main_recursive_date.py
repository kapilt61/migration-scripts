import json
from decimal import Decimal

from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from config.mongo_config import MongoConfig as mongo_config
from datetime import datetime
import dateutil.parser
import ijson

# === CONFIGURATION ===

MONGO_CONFIG = mongo_config.get()
ENV = MONGO_CONFIG['env']
ARANGO_DUMP_FILE = MONGO_CONFIG['dumpFile']
TLS = True
MONGO_URI = f"mongodb+srv://{MONGO_CONFIG['user']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}/?retryWrites=true&w=majority&appName=au-qa"
if ENV == 'local':
    TLS = False
    MONGO_URI = f"mongodb://{MONGO_CONFIG['user']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}/?retryWrites=true&w=majority&appName=au-qa"

MONGO_DB = MONGO_CONFIG['dbname']
MONGO_COLLECTION = MONGO_CONFIG['collection']
BATCH_SIZE = 100
MAX_WORKERS = 4
DATE_FIELDS = ['createdAt', 'updatedAt', 'timestamp', 'createdDt', 'modifiedDt', 'expireAt', 'quoteDate', 'createdDate', 'lastModifiedDate']  # Adjust as needed
VERSION_REQ = MONGO_CONFIG['versionReq']

EXCLUDE_DAT_FIELD = ['fromDate', 'toDate']

# === SETUP ===
client = MongoClient(MONGO_URI, tls=TLS, tlsallowinvalidcertificates=True)
collection = client[MONGO_DB][MONGO_COLLECTION]

# === UTILITIES ===
# def convert_arango_date(value):
#     if isinstance(value, str):
#         clean_value = value.split('[')[0]
#         try:
#             return dateutil.parser.isoparse(clean_value)
#         except Exception:
#             return value
#     elif isinstance(value, int) and len(str(value)) >= 12:
#         return datetime.utcfromtimestamp(value / 1000.0)
#     return value

def convert_arango_date(value):
    if isinstance(value, str):
        clean_value = value.split('[')[0]

        if clean_value.isdigit() or len(clean_value) < 12:
            return value

        try:
            parsed = dateutil.parser.isoparse(clean_value)
            return parsed
        except Exception:
            return value  # Not a valid date string
    # elif isinstance(value, int) and len(str(value)) >= 12:
    #     # Heuristic: likely a millisecond timestamp
    #     try:
    #         return datetime.utcfromtimestamp(value / 1000.0)
    #     except Exception:
    #         return value
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
def clean_and_prepare(doc):
    doc.pop('_id', None)   # Let Mongo auto-generate _id
    if MONGO_CONFIG['mapArangoFieldToMongo'] is True:
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
    doc.pop(ARANGO_FIELD, None)
    if VERSION_REQ:
        doc['version'] = 0
    doc = convert_decimals(doc)
    return process_dates(doc)



def read_json_in_batches(path, batch_size):
    batch = []
    with open(path, 'rb') as f:
        parser = ijson.items(f, 'item')  # Parses each object in the top-level array
        for doc in parser:
            try:
                doc = clean_and_prepare(doc)
                batch.append(doc)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            except Exception as e:
                print(f"Skipping document due to error: {e}")
        if batch:
            yield batch

def insert_batch(batch):
    try:
        # print(batch)
        # return 1
        result = collection.insert_many(batch, ordered=False)
        return len(result.inserted_ids)
    except BulkWriteError as bwe:
        errors = bwe.details.get('writeErrors', [])
        print(f"Bulk insert warning: {len(errors)} duplicates or errors")
        return len(bwe.details.get('writeResult', {}).get('insertedIds', []))

# === EXECUTION ===
# total = 0
# with ThreadPoolExecutor(MAX_WORKERS) as executor:
#     futures = [executor.submit(insert_batch, b) for b in read_json_in_batches(ARANGO_DUMP_FILE, BATCH_SIZE)]
#     for f in futures:
#         total += f.result()
total_inserted = 0
for batch in read_json_in_batches(ARANGO_DUMP_FILE, BATCH_SIZE):
    inserted = insert_batch(batch)
    total_inserted += inserted
    print(f"Inserted {inserted} documents (running total: {total_inserted})")
print(f"\n Done. Total documents inserted: {total_inserted}")
