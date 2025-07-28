# mongo_lock_test.py
import logging
import threading
import time
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

# Detailed logging configuration
logging.basicConfig(
    filename="mongo_lock_test.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S'
)

MONGO_URI = "mongodb://localhost:27100,localhost:27101,localhost:27102/?replicaSet=txntest"
DB_NAME = "flightdb"
COLL_NAME = "flights"
BOOK_SEAT_TIMEOUT = 15  # seconds

# Test flight data
FLIGHT_DOC = {
    "flight_no": "CA1234",
    "seat": "31A",
    "date": datetime.utcnow(),
    "price": 1999
}

def setup_collection():
    """Initialize the collection and insert a test seat document."""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    coll = db.get_collection(COLL_NAME)
    coll.delete_many({})
    coll.insert_one(FLIGHT_DOC, bypass_document_validation=True)
    logging.info("Collection initialized and test seat inserted: %s", FLIGHT_DOC)
    client.close()

def book_seat(session_name, hold_time=5, simulate_error=False):
    """
    Attempt to lock and book a seat within a transaction.
    'hold_time' controls how long the transaction holds the lock.
    'simulate_error=True' simulates a transaction interruption due to an error.
    """
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    coll = db.get_collection(COLL_NAME)
    session = client.start_session()
    try:
        start = time.time()
        logging.info("Starting transaction, preparing to book seat (%s)", session_name)
        with session.start_transaction(
            read_concern=ReadConcern("snapshot"),
            write_concern=WriteConcern("majority"),
        ):
            # Query the seat
            seat = coll.find_one({"flight_no": "CA1234", "seat": "31A"}, session=session)
            if not seat:
                logging.warning("[%s] Seat not found, aborting transaction.", session_name)
                return

            # Check if seat is already reserved
            if seat.get("reserved"):
                logging.warning("[%s] Seat already reserved, aborting transaction.", session_name)
                return

            # Mark reservation
            coll.update_one(
                {"_id": seat["_id"]},
                {"$set": {"reserved": session_name, "reserved_date": datetime.utcnow()}},
                session=session
            )
            logging.info("[%s] Holding transaction lock for %d seconds...", session_name, hold_time)
            time.sleep(hold_time)

            if simulate_error:
                raise RuntimeError("Simulated error: Transaction interrupted!")

            logging.info("[%s] Preparing to commit transaction.", session_name)
        elapsed = time.time() - start
        logging.info("[%s] Transaction committed successfully, lock held for %.2fs", session_name, elapsed)
    except PyMongoError as e:
        elapsed = time.time() - start
        logging.error("[%s] PyMongo exception: %s (transaction duration %.2fs)", session_name, str(e), elapsed)
    except Exception as e:
        elapsed = time.time() - start
        logging.error("[%s] Other exception: %s (transaction duration %.2fs)", session_name, str(e), elapsed)
    finally:
        session.end_session()
        client.close()

def block_writer(outside_name):
    """Attempt to directly modify the same seat outside a transaction and observe if it is blocked or fails."""
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[DB_NAME]
    coll = db.get_collection(COLL_NAME)
    try:
        logging.info("[%s] Attempting to directly write to seat outside transaction, observing for blocking...", outside_name)
        start = time.time()
        result = coll.update_one(
            {"flight_no": "CA1234", "seat": "31A"},
            {"$set": {"price": 2099, "last_modify_by": outside_name}}
        )
        elapsed = time.time() - start
        logging.info("[%s] Non-transactional write operation completed: matched %d, duration %.2fs",
                     outside_name, result.matched_count, elapsed)
    except Exception as e:
        logging.error("[%s] Write operation exception: %s", outside_name, str(e))
    finally:
        client.close()

def concurrent_booking_scenario():
    """Concurrent test: Transaction A holds the lock first, Transaction B attempts to book the same seat afterwards."""
    setup_collection()

    threads = []
    # Transaction A, acquires lock first, holds for 10 seconds
    t1 = threading.Thread(
        target=book_seat, args=("Transaction A", 10, False), name="Txn-A"
    )
    # After 2 seconds, Transaction B tries booking (will be blocked until A releases lock)
    t2 = threading.Thread(
        target=book_seat, args=("Transaction B", 2, False), name="Txn-B"
    )
    # After 1 second, non-transactional write tries to interrupt (will be blocked until A releases lock)
    t3 = threading.Thread(
        target=block_writer, args=("External Writer",), name="Writer-Out"
    )
    threads.extend([t1, t2, t3])

    t1.start()
    time.sleep(1)  # A got the lock, Writer-Out tries to enter but will be blocked by transaction
    t3.start()
    time.sleep(1)  # Writer-Out is waiting, B tries next, still blocked
    t2.start()

    # Wait for all threads to finish
    for t in threads:
        t.join()

    logging.info("Test completed. All logs are saved in mongo_lock_test.log.")

if __name__ == "__main__":
    concurrent_booking_scenario()
    print("All tests have been automatically executed. See mongo_lock_test.log for details.")
