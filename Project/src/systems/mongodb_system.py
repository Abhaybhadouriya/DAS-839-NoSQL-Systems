from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from src.oplog.oplog_manager import OpLogManager
from src.config import MONGO_CONFIG, OPLOG_PATHS
import time
from datetime import datetime

class MongoDBSystem:
    def __init__(self):
        self.oplog = OpLogManager(OPLOG_PATHS["mongo"])
        self.op_id = 1
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.client = MongoClient(f"mongodb://{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}")
                self.client.server_info()  # Test connection
                break
            except ConnectionError as e:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                raise Exception(f"Failed to connect to MongoDB after {max_retries} attempts: {e}")
        self.db = self.client[MONGO_CONFIG['database']]
        self.collection = self.db[MONGO_CONFIG['collection']]
        self.collection.create_index(
            [("admission_number", 1), ("subject", 1), ("period", 1)],
            unique=True
        )

    def insert(self, admission_number, subject, period, grade):
        timestamp = datetime.now().isoformat()
        self.collection.update_one(
            {"admission_number": admission_number, "subject": subject, "period": period},
            {"$set": {"grade": grade, "timestamp": timestamp}},
            upsert=True
        )
        self.oplog.log_operation(self.op_id, "INSERT", (admission_number, subject, period), grade, timestamp)
        self.op_id += 1

    def read(self, admission_number, subject, period):
        result = self.collection.find_one({
            "admission_number": admission_number,
            "subject": subject,
            "period": period
        })
        self.oplog.log_operation(self.op_id, "READ", (admission_number, subject, period), timestamp=datetime.now().isoformat())
        self.op_id += 1
        return result.get("grade") if result else None

    def update(self, admission_number, subject, period, grade):
        timestamp = datetime.now().isoformat()
        self.collection.update_one(
            {"admission_number": admission_number, "subject": subject, "period": period},
            {"$set": {"grade": grade, "timestamp": timestamp}}
        )
        self.oplog.log_operation(self.op_id, "UPDATE", (admission_number, subject, period), grade, timestamp)
        self.op_id += 1

    def delete(self, admission_number, subject, period):
        timestamp = datetime.now().isoformat()
        self.collection.delete_one({
            "admission_number": admission_number,
            "subject": subject,
            "period": period
        })
        self.oplog.log_operation(self.op_id, "DELETE", (admission_number, subject, period), timestamp=timestamp)
        self.op_id += 1

    def merge(self, other_system_name):
        oplog_path = OPLOG_PATHS.get(other_system_name.lower())
        print(other_system_name)
        if not oplog_path:
            print(f"Invalid system MG_MERGE: {other_system_name}")
            return

        other_oplog = OpLogManager(oplog_path)
        operations = other_oplog.read_log()

        for op in operations:
            if op["operation"] in ["INSERT", "UPDATE"] and op["grade"]:
                existing = self.collection.find_one({
                    "admission_number": op["admission_number"],
                    "subject": op["subject"],
                    "period": op["period"]
                })
                if not existing or (existing and existing.get("timestamp", "1970-01-01T00:00:00") < op["timestamp"]):
                    self.collection.update_one(
                        {
                            "admission_number": op["admission_number"],
                            "subject": op["subject"],
                            "period": op["period"]
                        },
                        {"$set": {"grade": op["grade"], "timestamp": op["timestamp"]}},
                        upsert=True
                    )
                    self.oplog.log_operation(self.op_id, op["operation"], (op["admission_number"], op["subject"], op["period"]), op["grade"], op["timestamp"])
                    self.op_id += 1
            elif op["operation"] == "DELETE":
                existing = self.collection.find_one({
                    "admission_number": op["admission_number"],
                    "subject": op["subject"],
                    "period": op["period"]
                })
                if existing and existing.get("timestamp", "1970-01-01T00:00:00") <= op["timestamp"]:
                    self.collection.delete_one({
                        "admission_number": op["admission_number"],
                        "subject": op["subject"],
                        "period": op["period"]
                    })
                    self.oplog.log_operation(self.op_id, "DELETE", (op["admission_number"], op["subject"], op["period"]), timestamp=op["timestamp"])
                    self.op_id += 1