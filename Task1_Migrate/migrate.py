#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""
ReturnHelper incremental migration tool (lightweight CLI based on business scenarios, supports breakpoint and multi-window management)
Features supported:
- Full sync (baseline window)
- Incremental sync (baseline window)
- Historical correction (correction window, supports forced overwrite)
- Breakpoint resume
- Status viewing
- Breakpoint reset (use with caution)
"""


import os
import json
import logging
import decimal
import mysql.connector
import boto3
from pymongo import MongoClient, UpdateOne
from bson.decimal128 import Decimal128
from datetime import datetime
from queue import Queue
import argparse
from dotenv import load_dotenv


# ======================= Load environment variables ==========================
load_dotenv()

ENV = os.getenv("ENVIRONMENT", "development").lower()

MYSQL_CONFIG = {
    "development": {
        "host": os.getenv("MYSQL_DEV_HOST"),
        "port": int(os.getenv("MYSQL_DEV_PORT", 3306)),
        "user": os.getenv("MYSQL_DEV_USER"),
        "password": os.getenv("MYSQL_DEV_PASSWORD"),
        "database": os.getenv("MYSQL_DEV_DATABASE"),
    },
    "production": {
        "host": os.getenv("MYSQL_PROD_HOST"),
        "port": int(os.getenv("MYSQL_PROD_PORT", 3306)),
        "user": os.getenv("MYSQL_PROD_USER"),
        "password": os.getenv("MYSQL_PROD_PASSWORD"),
        "database": os.getenv("MYSQL_PROD_DATABASE"),
    }
}

DYNAMODB_CONFIG = {
    "development": {
        "endpoint_url": os.getenv("DYNAMODB_DEV_ENDPOINT_URL"),
        "region_name": os.getenv("DYNAMODB_DEV_REGION_NAME"),
        "table": os.getenv("DYNAMODB_DEV_TABLE"),
        "access_key": os.getenv("DYNAMODB_DEV_ACCESS_KEY"),
        "secret_key": os.getenv("DYNAMODB_DEV_SECRET_KEY"),
    },
    "production": {
        "endpoint_url": os.getenv("DYNAMODB_PROD_ENDPOINT_URL"),
        "region_name": os.getenv("DYNAMODB_PROD_REGION_NAME"),
        "table": os.getenv("DYNAMODB_PROD_TABLE"),
        "access_key": os.getenv("DYNAMODB_PROD_ACCESS_KEY"),
        "secret_key": os.getenv("DYNAMODB_PROD_SECRET_KEY"),
    }
}

MONGO_CONFIG = {
    "development": {
        "uri": os.getenv("MONGO_DEV_URI"),
        "db": os.getenv("MONGO_DEV_DB"),
        "transactions": os.getenv("MONGO_DEV_TRANSACTIONS"),
    },
    "production": {
        "uri": os.getenv("MONGO_PROD_URI"),
        "db": os.getenv("MONGO_PROD_DB"),
        "transactions": os.getenv("MONGO_PROD_TRANSACTIONS"),
    }
}


# Set configs based on environment
MYSQL = MYSQL_CONFIG.get(ENV)
DYNAMODB = DYNAMODB_CONFIG.get(ENV)
MONGO = MONGO_CONFIG.get(ENV)


RETURN_INVENTORY_THRESHOLD = 200
BUCKET_SIZE = 200
BATCH_SIZE = 500
CHECKPOINT_FILE = "sync_checkpoint.json"
LOG_DIR = "logs"


TRANSACTION_TYPE_KEY_AND_BUCKET = {
    "rmh": ("returnInventoryList", "rmh_buckets"),
    "CONSOLIDATE_SHIPPING_MERGE": ("returnInventoryList", "consolidate_shipping_merge_buckets"),
    "fbd": ("fbaWarehouseInventoryList", "fbd_buckets"),
    "rtp": ("recallList", "rtp_buckets"),
}


# ========== Logging configuration ===========
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, f"migrate_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("migrate")


# ========== Connection initialization ===========
mysql_conn = mysql.connector.connect(**MYSQL)
mysql_cursor = mysql_conn.cursor(dictionary=True)


dynamodb = boto3.resource(
    "dynamodb",
    endpoint_url=DYNAMODB["endpoint_url"],
    region_name=DYNAMODB["region_name"],
    aws_access_key_id=DYNAMODB["access_key"],
    aws_secret_access_key=DYNAMODB["secret_key"]
)
dynamo_table = dynamodb.Table(DYNAMODB["table"])


mongo = MongoClient(MONGO["uri"])
mongo_db = mongo[MONGO["db"]]
tx_col = mongo_db[MONGO["transactions"]]
tx_col.create_index("apiTransactionId", unique=True)
tx_col.create_index("createOn")


# Dynamically create references to all Buckets collections for batch insert convenience
bucket_collections = {}
for ttype, (_, bucket_name) in TRANSACTION_TYPE_KEY_AND_BUCKET.items():
    bucket_collections[bucket_name] = mongo_db[bucket_name]
    # Create index on parentTransactionNumber for efficient queries
    bucket_collections[bucket_name].create_index("parentTransactionNumber")


# ========== Utility functions ==========


def to_decimal128(val):
    """
    Recursively convert decimal.Decimal to bson.Decimal128 for Mongo storage
    """
    if isinstance(val, decimal.Decimal):
        return Decimal128(str(val))
    if isinstance(val, list):
        return [to_decimal128(i) for i in val]
    if isinstance(val, dict):
        return {k: to_decimal128(v) for k, v in val.items()}
    return val


def load_checkpoint(file_path):
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "base_windows": [],
        "correction_windows": []
    }


def save_checkpoint(data, file_path):
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)


def load_dynamodb():
    """
    Scan DynamoDB table and return a dictionary mapping transactionNumber -> meta data
    """
    lookup, total, last_key = {}, 0, None
    while True:
        params = {"Limit": 1000}
        if last_key:
            params["ExclusiveStartKey"] = last_key
        resp = dynamo_table.scan(**params)
        items = resp.get("Items", [])
        for item in items:
            # Use transactionNumber as primary association key
            transaction_number = item.get("transactionNumber") or item.get("transaction_number")
            if transaction_number:
                lookup[str(transaction_number)] = item
        total += len(items)
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
    return lookup


def read_mysql_batches(window, checkpoint=None):
    """
    Read MySQL data in batches based on window and checkpoint
    """
    start, end = window["start"], window["end"]
    cond = "create_on >= %s AND create_on < %s"
    params = [start, end]
    # Sort and checkpoint on create_on, transaction_number
    if checkpoint and checkpoint.get("last_checkpoint_time") and checkpoint.get("last_checkpoint_id"):
        t, tid = checkpoint["last_checkpoint_time"], checkpoint["last_checkpoint_id"]
        cond += " AND ((create_on > %s) OR (create_on = %s AND transaction_number > %s))"
        params.extend([t, t, tid])
    query = f"""
        SELECT api_transaction_id, transaction_number, api_id, currency_code,
               transaction_type, head_id, amount, notes, create_on, create_by
        FROM ApiTransaction
        WHERE {cond}
        ORDER BY create_on, transaction_number
    """
    mysql_cursor.execute(query, params)
    rows = mysql_cursor.fetchall()
    for i in range(0, len(rows), BATCH_SIZE):
        yield rows[i:i + BATCH_SIZE]


def transform_data(mysql_row, dy_meta, buckets_map):
    """
    Merge MySQL and DynamoDB metadata, convert data to Mongo storage format.
    - apiTransactionId is replaced by ULID in DynamoDB meta (fallback to transactionNumber)
    - Bucket collection's parentTransactionId changed to parentTransactionNumber (main table transactionNumber)
    """
    # Get ULID
    tid = str(mysql_row["transaction_number"])
    ulid = None
    if dy_meta and dy_meta.get("apiTransactionId"):
        ulid = dy_meta["apiTransactionId"]
    else:
        ulid = tid


    txn_type = mysql_row["transaction_type"]


    doc = {
        "apiTransactionId": ulid,
        "transactionNumber": mysql_row["transaction_number"],
        "apiId": mysql_row["api_id"],
        "currencyCode": mysql_row["currency_code"].upper(),
        "transactionType": txn_type,
        "headId": str(mysql_row["head_id"]),
        "amount": Decimal128(str(mysql_row["amount"])),
        "notes": mysql_row.get("notes", ""),
        "createOn": mysql_row["create_on"],
        "createBy": mysql_row["create_by"],
        "metadata": {}
    }


    if dy_meta:
        if txn_type in TRANSACTION_TYPE_KEY_AND_BUCKET:
            key, bucket_collection_name = TRANSACTION_TYPE_KEY_AND_BUCKET[txn_type]
            if key in dy_meta and isinstance(dy_meta[key], list) and len(dy_meta[key]) > 0:
                val_list = dy_meta[key]
                chunks = [val_list[i:i + BUCKET_SIZE] for i in range(0, len(val_list), BUCKET_SIZE)]
                for c in chunks:
                    bucket_doc = {
                        "parentTransactionNumber": tid,  # Link to main table by transactionNumber
                        key: to_decimal128(c),
                        "createOn": mysql_row["create_on"],
                        "transactionType": txn_type,
                    }
                    buckets_map.setdefault(bucket_collection_name, []).append(bucket_doc)
            else:
                for k, v in dy_meta.items():
                    if k in TRANSACTION_TYPE_KEY_AND_BUCKET.values():
                        continue
                    if k != key:
                        doc["metadata"][k] = to_decimal128(v)
        else:
            for k, v in dy_meta.items():
                if k == "apiTransactionId":
                    continue
                doc["metadata"][k] = to_decimal128(v)


    return doc


def process_batch(batch, dy_lookup):
    """
    Process MySQL batch data and sync to Mongo:
    - Transform data
    - Insert into transactions collection
    - Insert into corresponding Buckets collections
    """
    docs = []
    buckets_map = {}


    last_ck_time, last_ck_id = None, None


    for row in batch:
        try:
            dy_meta = dy_lookup.get(str(row["transaction_number"]))
            doc = transform_data(row, dy_meta, buckets_map)
            docs.append(doc)
            last_ck_time, last_ck_id = row["create_on"], row["transaction_number"]
        except Exception as e:
            logger.error(f"Data transformation error transaction_number={row['transaction_number']}. {e}")


    try:
        if docs:
            ops = [UpdateOne({"apiTransactionId": d["apiTransactionId"]}, {"$set": d}, upsert=True) for d in docs]
            tx_col.bulk_write(ops, ordered=False)
    except Exception as e:
        logger.error(f"Transaction batch write error. {e}")


    for collection_name, bucket_docs in buckets_map.items():
        if bucket_docs:
            try:
                bucket_collections[collection_name].insert_many(bucket_docs, ordered=False)
            except Exception as e:
                logger.error(f"Buckets write error, collection [{collection_name}]. {e}")


    return last_ck_time, last_ck_id, len(docs)


def migrate_window(window, dy_lookup, checkpoint_state, cp_data_section, section_name):
    """
    Migrate data of specified time window, supports breakpoint resume.
    """
    logger.info(f"Starting migration {section_name} window: {window['start']} ~ {window['end']}")


    mysql_count_query = """
    SELECT COUNT(*) AS c
    FROM ApiTransaction
    WHERE create_on >= %s AND create_on < %s
    """
    mysql_cursor.execute(mysql_count_query, (window["start"], window["end"]))
    total_count = mysql_cursor.fetchone()["c"]
    logger.info(f"Window data volume check: total {total_count} records in MySQL for this window.")


    if total_count == 0:
        logger.info(f"{section_name} window-{window['start']}~{window['end']} has no data, skipping DynamoDB load and migration.")
        checkpoint_state.update({
            "status": "completed",
            "processed_count": 0,
            "last_update_time": datetime.now().isoformat(),
            "owner": os.uname().nodename,
            "last_checkpoint_time": None,
            "last_checkpoint_id": None,
            "start_exec_time": datetime.now().isoformat(),
            "finish_exec_time": datetime.now().isoformat(),
            "duration_seconds": 0,
        })
        save_checkpoint(cp_data, CHECKPOINT_FILE)
        return


    logger.info("Loading DynamoDB metadata...")
    dy_lookup.clear()
    dy_lookup.update(load_dynamodb())
    logger.info(f"Loaded {len(dy_lookup)} DynamoDB metadata items.")


    processed = checkpoint_state.get("processed_count", 0)
    last_ck_time, last_ck_id = checkpoint_state.get("last_checkpoint_time"), checkpoint_state.get("last_checkpoint_id")
    start_exec_time = datetime.now()


    batch_queue = Queue()
    for batch in read_mysql_batches(window, checkpoint_state):
        batch_queue.put(batch)


    while not batch_queue.empty():
        batch = batch_queue.get()


        checkpoint_state.update({
            "status": "in_progress",
            "owner": os.uname().nodename,
            "last_checkpoint_time": last_ck_time,
            "last_checkpoint_id": last_ck_id,
            "processed_count": processed,
            "last_update_time": datetime.now().isoformat(),
        })
        save_checkpoint(cp_data, CHECKPOINT_FILE)


        last_ck_time, last_ck_id, batch_num = process_batch(batch, dy_lookup)
        processed += batch_num
        batch_queue.task_done()


        checkpoint_state["last_checkpoint_time"] = last_ck_time
        checkpoint_state["last_checkpoint_id"] = last_ck_id
        checkpoint_state["processed_count"] = processed


    finish_exec_time = datetime.now()
    checkpoint_state.update({
        "status": "completed",
        "finish_exec_time": finish_exec_time.isoformat(),
        "start_exec_time": start_exec_time.isoformat(),
        "duration_seconds": int((finish_exec_time - start_exec_time).total_seconds()),
        "processed_count": processed,
        "last_update_time": finish_exec_time.isoformat(),
        "owner": os.uname().nodename,
    })
    save_checkpoint(cp_data, CHECKPOINT_FILE)
    logger.info(f"{section_name} window-{window['start']}~{window['end']} sync completed, total migrated {processed} records.")


def check_consistency(window):
    """
    Simple consistency check comparing record counts in MySQL and MongoDB
    """
    start_str, end_str = window["start"], window["end"]
    start_dt = datetime.fromisoformat(start_str)
    end_dt = datetime.fromisoformat(end_str)


    mysql_cursor.execute(
        "SELECT COUNT(*) AS c FROM ApiTransaction WHERE create_on >= %s AND create_on < %s",
        (start_str, end_str))
    mysql_count = mysql_cursor.fetchone()["c"]


    mongo_count = tx_col.count_documents({"createOn": {"$gte": start_dt, "$lt": end_dt}})


    logger.info(f"Consistency check: window[{start_str}~{end_str}] MySQL={mysql_count}, MongoDB={mongo_count}")
    if mysql_count != mongo_count:
        logger.warning(f"Count mismatch, manual investigation recommended!")
    else:
        logger.info("Data counts are consistent.")


def parse_args():
    parser = argparse.ArgumentParser(
        description="ReturnHelper incremental migration tool (simple version for business scenarios)"
    )


    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--full-sync", action="store_true",
                       help="Full sync, from 1970-01-01 to specified or current time")
    group.add_argument("--incremental", action="store_true",
                       help="Incremental sync, from breakpoint to specified or current time")
    group.add_argument("--correction", action="store_true",
                       help="Correction window, requires --start and --end")
    group.add_argument("--resume", action="store_true", help="Resume broken migration windows")
    group.add_argument("--show-status", action="store_true", help="Show breakpoint status")
    group.add_argument("--gen-report", action="store_true", help="Generate migration report")
    group.add_argument("--reset", action="store_true", help="Reset breakpoints (dangerous, confirm required)")


    parser.add_argument("--start", type=str, default=None,
                        help="Correction window start time, ISO format, required for --correction")
    parser.add_argument("--end", type=str, default=None,
                        help="End time, ISO format or 'now', default current time")
    parser.add_argument("--force", action="store_true",
                        help="Force overwrite, must be used with --correction")


    args = parser.parse_args()


    # Parse end time
    if args.end is None:
        args.end = datetime.now().isoformat()
    elif args.end.lower() == "now":
        args.end = datetime.now().isoformat()


    # Validate correction args
    if args.correction and (not args.start or not args.end):
        parser.error("--correction mode requires both --start and --end")


    if args.force and not args.correction:
        parser.error("--force option must be used with --correction")


    return args


def main():
    global cp_data


    args = parse_args()
    cp_data = load_checkpoint(CHECKPOINT_FILE)


    if args.reset:
        confirm = input("WARNING: Resetting breakpoints will clear all migration progress. Type Y to confirm: ")
        if confirm.strip().upper() == "Y":
            if os.path.exists(CHECKPOINT_FILE):
                os.remove(CHECKPOINT_FILE)
            cp_data = load_checkpoint(CHECKPOINT_FILE)
            logger.info("Breakpoint file has been reset.")
        else:
            logger.info("Cancelled breakpoint reset operation.")
            return


    if args.full_sync and not cp_data.get("base_windows"):
        baseline = {
            "start": "1970-01-01T00:00:00",
            "end": args.end,
            "status": "pending",
            "owner": None,
            "processed_count": 0,
            "last_checkpoint_time": None,
            "last_checkpoint_id": None,
            "last_update_time": None,
        }
        cp_data["base_windows"].append(baseline)
        save_checkpoint(cp_data, CHECKPOINT_FILE)


    if args.correction:
        new_window = {
            "start": args.start,
            "end": args.end,
            "status": "pending",
            "owner": None,
            "processed_count": 0,
            "last_checkpoint_time": None,
            "last_checkpoint_id": None,
            "last_update_time": None,
            "resync": args.force,
        }
        cp_data.setdefault("correction_windows", []).append(new_window)
        save_checkpoint(cp_data, CHECKPOINT_FILE)
        logger.info(f"Added correction window {args.start} ~ {args.end}" + (", force overwrite" if args.force else ""))


    dy_lookup = {}


    if args.full_sync or args.incremental:
        logger.info("Starting migration: baseline windows section...")
        for window in cp_data.get("base_windows", []):
            if window.get("status") in ("pending", "in_progress"):
                migrate_window(window, dy_lookup, window, cp_data["base_windows"], "base_windows")
                check_consistency(window)


    if args.correction:
        logger.info("Starting migration: correction windows section...")
        for window in cp_data.get("correction_windows", []):
            if window.get("status") in ("pending", "in_progress"):
                migrate_window(window, dy_lookup, window, cp_data["correction_windows"], "correction_windows")
                check_consistency(window)


    if args.resume:
        logger.info("Resuming breakpoints, completing unfinished windows...")
        for section_name in ("base_windows", "correction_windows"):
            for window in cp_data.get(section_name, []):
                if window.get("status") in ("pending", "in_progress"):
                    migrate_window(window, dy_lookup, window, cp_data[section_name], section_name)
                    check_consistency(window)


    if args.show_status:
        print(json.dumps(cp_data, ensure_ascii=False, indent=2, default=str))
        return


    logger.info("All migration windows synchronized and checked, please review breakpoint file and logs for details.")


if __name__ == "__main__":
    main()
