#!/usr/bin/env python3
"""
txn_monitor.py


MongoDB Transaction Monitoring and Testing Script
Features:
- Simulate multi-collection transactional writes (seat booking, payment, audit)
- Listen to change streams on specified collections
- Periodically monitor primary/secondary node status of the replica set, printing failover logs
- Support transaction retry mechanism, printing detailed logs
- Use official recommended transaction read/write concerns
- Suitable for verifying transaction behavior under extreme scenarios (primary node failure)


Dependencies:
- pymongo >= 4.0
- MongoDB replica set environment (3-node replica set required)
- Install dependencies via virtual environment or globally


Usage:
python txn_monitor.py
"""
import pymongo
from pymongo import errors, WriteConcern, ReadPreference
from pymongo.read_concern import ReadConcern
from pymongo.errors import ConnectionFailure, OperationFailure
from datetime import datetime
import time
import random
import threading
import subprocess


def kill_primary_node():
    print("[AutoKill] Preparing to execute primary node kill")


    def run_kill():
        print("[AutoKill] Kill thread started")
        subprocess.call(['python3', 'kill_primary.py'])  # Adjust path accordingly
        print("[AutoKill] Kill thread finished")


    t = threading.Thread(target=run_kill)
    t.start()


    print("[AutoKill] Kill thread launched")


def log(msg):
    """Unified log printing with timestamp and thread name for easy multithreading observation"""
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    thread_name = threading.current_thread().name
    print(f"[{now}][{thread_name}] {msg}", flush=True)


def monitor_primary_secondary_status(uri, interval=5):
    client = pymongo.MongoClient(uri)
    while True:
        try:
            status = client.admin.command("hello")
            is_primary = status.get("isWritablePrimary", False)
            hosts = status.get("hosts", [])
            primary = status.get("primary", "Unknown")
            me = status.get("me", "Unknown")
            state = "PRIMARY" if is_primary else "SECONDARY"
            log(f"Node status: {me} State: {state} Current primary: {primary} Replica set members: {hosts}")
        except Exception as e:
            log(f"Error querying replica set status: {e}")
        time.sleep(interval)


def monitor_changes(uri, db_name, coll_name):
    client = pymongo.MongoClient(uri)
    coll = client[db_name][coll_name]
    log(f"[Monitor] Started listening to {db_name}.{coll_name}, press Ctrl+C to stop")
    try:
        with coll.watch() as stream:
            for change in stream:
                op = change.get("operationType")
                cluster_time = change.get("clusterTime").as_datetime() if change.get("clusterTime") else None
                doc = change.get("fullDocument")
                if doc and "_id" in doc:
                    doc.pop("_id")
                log(f"[Change] Operation: {op}, ClusterTime: {cluster_time}, Document changed: {doc}")
    except KeyboardInterrupt:
        log("[Monitor] Stopped listening.")
    except Exception as e:
        log(f"[Monitor][ERROR] Exception while listening: {e}")


def commit_with_retry(session):
    while True:
        try:
            session.commit_transaction()
            log("[Txn] Transaction committed successfully")
            #kill_primary_node()  # Uncomment to kill primary after every successful commit
            break
        except (ConnectionFailure, OperationFailure) as exc:
            if exc.has_error_label("UnknownTransactionCommitResult"):
                log("[Txn] UnknownTransactionCommitResult, retrying commit...")
                continue
            else:
                log(f"[Txn][ERROR] Commit failed: {exc}")
                raise


def run_transaction_with_retry(client, txn_func):
    txn_options = {
        "read_concern": ReadConcern("majority"),
        "write_concern": WriteConcern("majority", j=True),
        "read_preference": ReadPreference.PRIMARY,
    }
    with client.start_session() as session:
        while True:
            try:
                session.start_transaction(**txn_options)
                result = txn_func(session)
                commit_with_retry(session)
                return result
            except (ConnectionFailure, OperationFailure) as exc:
                if exc.has_error_label("TransientTransactionError"):
                    log("[Txn] TransientTransactionError, retrying transaction, aborting current transaction first...")
                    try:
                        session.abort_transaction()
                    except Exception as abort_exc:
                        log(f"[Txn][WARN] Transaction abort failed: {abort_exc}")
                    time.sleep(0.1)
                    continue
                else:
                    try:
                        session.abort_transaction()
                    except Exception as abort_exc:
                        log(f"[Txn][WARN] Transaction abort failed: {abort_exc}")
                    raise


def transactional_write_example(client, db_name, seats_coll_name, payments_coll_name, audit_coll_name, seat_no):
    seats = client[db_name][seats_coll_name]
    payments = client[db_name][payments_coll_name]
    audit = client[db_name][audit_coll_name]


    price = random.randint(200, 500)
    seat_str = f"{seat_no}A"
    log(f"[Txn] Booking seat {seat_str}, price {price}")


    def txn_logic(session):
        seats.insert_one({
            "flight_no": "EI178",
            "seat": seat_str,
            "date": datetime.utcnow()
        }, session=session)


        delay = random.uniform(0.5, 1.2)
        log(f"[Txn] Simulating payment delay {delay:.2f} seconds")
        time.sleep(delay)


        payments.insert_one({
            "flight_no": "EI178",
            "seat": seat_str,
            "date": datetime.utcnow(),
            "price": price
        }, session=session)


        audit.update_one({"audit": "seats"}, {"$inc": {"count": 1}}, upsert=True, session=session)
        return delay


    return run_transaction_with_retry(client, txn_logic)


def main():
    mongodb_uri = "mongodb://localhost:27100,localhost:27101,localhost:27102/?replicaSet=txntest&retryWrites=true"
    db_name = "SEATSDB"
    seats_coll_name = "seats"
    payments_coll_name = "payments"
    audit_coll_name = "audit"


    client = pymongo.MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000)


    try:
        client.admin.command('ping')
        log("[Info] MongoDB connected successfully")
    except Exception as e:
        log(f"[Error] Unable to connect to MongoDB: {e}")
        return


    log("[Init] Cleaning test data collections...")
    client[db_name][seats_coll_name].drop()
    client[db_name][payments_coll_name].drop()
    client[db_name][audit_coll_name].drop()


    client[db_name].create_collection(seats_coll_name)
    client[db_name].create_collection(payments_coll_name)
    client[db_name].create_collection(audit_coll_name)
    log("[Init] Collections ready")


    monitor_thread = threading.Thread(target=monitor_changes, args=(mongodb_uri, db_name, seats_coll_name), daemon=True, name="ChangeMonitor")
    monitor_thread.start()


    status_thread = threading.Thread(target=monitor_primary_secondary_status, args=(mongodb_uri,), daemon=True, name="StatusMonitor")
    status_thread.start()


    log("[Main] Starting transactional writes; press Ctrl+C to stop")
    seat_counter = 1


    try:
        while True:
            transactional_write_example(client, db_name, seats_coll_name, payments_coll_name, audit_coll_name, seat_counter)
            seat_counter += 1
            time.sleep(0.5)
    except KeyboardInterrupt:
        log("[Main] User terminated program")


if __name__ == "__main__":
    main()
