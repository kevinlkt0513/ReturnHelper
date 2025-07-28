#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""
ApiTransaction MongoDB Create-Read-Update (no delete) writing utility example
Supports:
- Main data insertion and associated Meta Bucket sharded writing
- Uses transactionNumber as main reference key, parentTransactionNumber to link Buckets
- Uses pymongo 4.x, no replica set transaction support required
- Detailed exception handling for precise fault localization and operations
- CRU operations are non-atomic
"""


import logging
from pymongo import MongoClient, WriteConcern
from pymongo.errors import PyMongoError, OperationFailure, ConnectionFailure, ConfigurationError
from bson.decimal128 import Decimal128
import decimal
from datetime import datetime


# ===================== Configuration =======================
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "xxx"
TX_COLLECTION = "transactions"
META_BUCKETS_CONFIG = {
    "rmh": ("returnInventoryList", "rmh_buckets"),
    "CONSOLIDATE_SHIPPING_MERGE": ("returnInventoryList", "consolidate_shipping_merge_buckets"),
    "fbd": ("fbaWarehouseInventoryList", "fbd_buckets"),
    "rtp": ("recallList", "rtp_buckets"),
}
BUCKET_SIZE = 200


# ========== Logging Configuration ===========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("api_transaction_dual_write")


# ================ Mongo Connection and Initialization ================
mongo = MongoClient(MONGO_URI)
db = mongo[DB_NAME]
tx_col = db[TX_COLLECTION]
bucket_collections = {}
for _, bucket_name in META_BUCKETS_CONFIG.values():
    col = db[bucket_name]
    col.create_index("parentTransactionNumber")
    bucket_collections[bucket_name] = col


# ============= Utility Functions =======================
def to_decimal128(val):
    """Recursively convert decimal.Decimal to Decimal128"""
    if isinstance(val, decimal.Decimal):
        return Decimal128(str(val))
    if isinstance(val, list):
        return [to_decimal128(i) for i in val]
    if isinstance(val, dict):
        return {k: to_decimal128(v) for k, v in val.items()}
    return val


def get_bucket_info(transaction_type):
    """Get the bucket configuration for a given transaction type"""
    return META_BUCKETS_CONFIG.get(transaction_type)


def get_bucket_collection(bucket_name):
    """Get the bucket collection object"""
    return db[bucket_name]


# =================== CRU Operations =====================


def create_api_transaction(api_tx_doc, meta_doc=None):
    """
    Create a new ApiTransaction and corresponding meta buckets
    """
    transaction_type = api_tx_doc.get("transactionType")
    transaction_number = api_tx_doc.get("transactionNumber")
    if not (transaction_type and transaction_number):
        raise ValueError("transactionType and transactionNumber are required")


    # Convert amount fields to Decimal128
    for k in ("amount",):
        if k in api_tx_doc and not isinstance(api_tx_doc[k], Decimal128):
            api_tx_doc[k] = Decimal128(str(api_tx_doc[k]))


    try:
        # Insert main document
        tx_col.with_options(write_concern=WriteConcern(w="majority")).insert_one(api_tx_doc)


        # If meta_doc and bucket config exist, insert bucket documents
        bucket_config = get_bucket_info(transaction_type)
        if bucket_config and meta_doc and bucket_config[0] in meta_doc:
            key, bucket_collection_name = bucket_config
            collection = get_bucket_collection(bucket_collection_name)
            meta_list = meta_doc[key]
            chunks = [meta_list[i:i+BUCKET_SIZE] for i in range(0, len(meta_list), BUCKET_SIZE)]
            for chunk in chunks:
                bucket_doc = {
                    "parentTransactionNumber": transaction_number,
                    key: to_decimal128(chunk),
                    "createOn": api_tx_doc.get("createOn", datetime.utcnow()),
                    "transactionType": transaction_type,
                }
                collection.with_options(write_concern=WriteConcern(w="majority")).insert_one(bucket_doc)
        return {"status": "ok", "transactionNumber": transaction_number}
    except OperationFailure as e:
        logger.error(f"Mongo OperationFailure during create: {e.details if hasattr(e, 'details') else str(e)}")
        raise
    except ConnectionFailure as e:
        logger.error(f"MongoDB network failure during create: {e}")
        raise
    except ConfigurationError as e:
        logger.error(f"MongoDB configuration error during create: {e}")
        raise
    except PyMongoError as e:
        logger.error(f"MongoDB general error during create: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected system error during create: {e}")
        raise


def update_api_transaction(transaction_number, update_doc, meta_doc=None):
    """
    Update main transaction and corresponding meta buckets
    """
    main_query = {"transactionNumber": transaction_number}
    main_exist = tx_col.find_one(main_query)
    if not main_exist:
        raise ValueError(f"transactionNumber {transaction_number} does not exist, cannot update")


    transaction_type = main_exist.get("transactionType")
    if not transaction_type:
        raise ValueError(f"transactionNumber {transaction_number} main document missing transactionType field")


    try:
        # Update main document
        tx_col.with_options(write_concern=WriteConcern(w="majority")).update_one(main_query, {"$set": update_doc})


        # If meta_doc and bucket config exist, delete old bucket docs then insert new ones
        bucket_config = get_bucket_info(transaction_type)
        if bucket_config and meta_doc and bucket_config[0] in meta_doc:
            key, bucket_collection_name = bucket_config
            collection = get_bucket_collection(bucket_collection_name)
            collection.with_options(write_concern=WriteConcern(w="majority")).delete_many({"parentTransactionNumber": transaction_number})
            meta_list = meta_doc[key]
            chunks = [meta_list[i:i+BUCKET_SIZE] for i in range(0, len(meta_list), BUCKET_SIZE)]
            for chunk in chunks:
                bucket_doc = {
                    "parentTransactionNumber": transaction_number,
                    key: to_decimal128(chunk),
                    "createOn": update_doc.get("createOn", datetime.utcnow()),
                    "transactionType": transaction_type,
                }
                collection.insert_one(bucket_doc)
        return {"status": "ok", "transactionNumber": transaction_number}
    except OperationFailure as e:
        logger.error(f"Mongo OperationFailure during update: {e.details if hasattr(e, 'details') else str(e)}")
        raise
    except ConnectionFailure as e:
        logger.error(f"MongoDB network failure during update: {e}")
        raise
    except ConfigurationError as e:
        logger.error(f"MongoDB configuration error during update: {e}")
        raise
    except PyMongoError as e:
        logger.error(f"MongoDB general error during update: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected system error during update: {e}")
        raise


def get_api_transaction_with_meta(transaction_number):
    """
    Query the main transaction and associated meta buckets by transactionNumber, return aggregated result
    """
    try:
        doc = tx_col.find_one({"transactionNumber": transaction_number})
        if not doc:
            return None


        transaction_type = doc.get("transactionType")
        meta = {}
        bucket_config = get_bucket_info(transaction_type)
        if bucket_config:
            key, bucket_collection_name = bucket_config
            collection = get_bucket_collection(bucket_collection_name)
            buckets_cursor = collection.find({"parentTransactionNumber": transaction_number})
            merged_list = []
            for b in buckets_cursor:
                if key in b:
                    merged_list.extend(b[key])
            meta[key] = merged_list
        doc["metaBuckets"] = meta
        return doc
    except PyMongoError as e:
        logger.error(f"MongoDB error during read: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during read: {e}")
        raise


# ================ Test Example ================


if __name__ == "__main__":
    from pprint import pprint
    from bson.decimal128 import Decimal128


    # Create example
    try:
        result = create_api_transaction({
            "apiTransactionId": "01F8MECHZX3TBDSZ7XRADM79XE",  # Example ULID
            "transactionNumber": "T2501072402",
            "apiId": 269,
            "currencyCode": "USD",
            "transactionType": "rmh",
            "headId": "head-uuid-string",
            "amount": Decimal128("200.00"),
            "notes": "charge sample",
            "createOn": datetime.utcnow(),
            "createBy": "sysop"
        }, meta_doc={
            "returnInventoryList": [{"item": "foo", "qty": 1}, {"item": "bar", "qty": 2}]
        })
        print("Create:", result)
    except Exception as e:
        print("Create failed:", e)


    # Read example
    try:
        doc = get_api_transaction_with_meta("T2501072402")
        print("\nRead:")
        pprint(doc)
    except Exception as e:
        print("Read failed:", e)


    # Update example
    try:
        update_result = update_api_transaction(
            transaction_number="T2501072402",
            update_doc={"notes": "update note", "amount": Decimal128("300.22")},
            meta_doc={"returnInventoryList": [{"item": "foo", "qty": 3}]}
        )
        print("\nUpdate:", update_result)
    except Exception as e:
        print("Update failed:", e)


    # Read after update to verify
    try:
        doc = get_api_transaction_with_meta("T2501072402")
        print("\nRead after update:")
        pprint(doc)
    except Exception as e:
        print("Read after update failed:", e)
