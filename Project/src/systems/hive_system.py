from pyhive import hive
from src.oplog.oplog_manager import OpLogManager
from src.config import HIVE_CONFIG, OPLOG_PATHS
import time

class HiveSystem:
    def __init__(self):
        self.oplog = OpLogManager(OPLOG_PATHS["hive"])
        self.op_id = 1
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.conn = hive.connect(host=HIVE_CONFIG['host'], port=HIVE_CONFIG['port'], database=HIVE_CONFIG['database'])
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                raise Exception(f"Failed to connect to Hive after {max_retries} attempts: {e}")
        self.table = HIVE_CONFIG['table']
        with self.conn.cursor() as cursor:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table} (
                    admission_number STRING,
                    subject STRING,
                    period STRING,
                    grade STRING,
                    timestamp STRING
                )
                PARTITIONED BY (admission_number, subject, period)
            """)

    def insert(self, admission_number, subject, period, grade):
        timestamp = datetime.now().isoformat()
        with self.conn.cursor() as cursor:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS temp_grade_roster AS
                SELECT admission_number, subject, period, grade, timestamp
                FROM {self.table}
                WHERE NOT (admission_number = '{admission_number}' 
                        AND subject = '{subject}' 
                        AND period = '{period}')
            """)
            cursor.execute(f"""
                INSERT INTO temp_grade_roster
                SELECT '{admission_number}', '{subject}', '{period}', '{grade}', '{timestamp}'
                WHERE NOT EXISTS (
                    SELECT 1 FROM {self.table}
                    WHERE admission_number = '{admission_number}' 
                    AND subject = '{subject}' 
                    AND period = '{period}'
                )
            """)
            cursor.execute(f"""
                INSERT OVERWRITE TABLE {self.table}
                SELECT * FROM temp_grade_roster
            """)
            cursor.execute("DROP TABLE IF EXISTS temp_grade_roster")
        self.conn.commit()
        self.oplog.log_operation(self.op_id, "INSERT", (admission_number, subject, period), grade, timestamp)
        self.op_id += 1

    def read(self, admission_number, subject, period):
        with self.conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT grade FROM {self.table} 
                WHERE admission_number = '{admission_number}' 
                AND subject = '{subject}' 
                AND period = '{period}'
            """)
            result = cursor.fetchone()
        self.oplog.log_operation(self.op_id, "READ", (admission_number, subject, period), timestamp=datetime.now().isoformat())
        self.op_id += 1
        return result[0] if result else None

    def update(self, admission_number, subject, period, grade):
        timestamp = datetime.now().isoformat()
        with self.conn.cursor() as cursor:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS temp_grade_roster AS
                SELECT admission_number, subject, period, grade, timestamp
                FROM {self.table}
            """)
            cursor.execute(f"""
                INSERT OVERWRITE TABLE temp_grade_roster
                SELECT admission_number, subject, period,
                       CASE WHEN admission_number = '{admission_number}' 
                            AND subject = '{subject}' 
                            AND period = '{period}' THEN '{grade}'
                            ELSE grade END,
                       CASE WHEN admission_number = '{admission_number}' 
                            AND subject = '{subject}' 
                            AND period = '{period}' THEN '{timestamp}'
                            ELSE timestamp END
                FROM temp_grade_roster
            """)
            cursor.execute(f"""
                INSERT OVERWRITE TABLE {self.table}
                SELECT * FROM temp_grade_roster
            """)
            cursor.execute("DROP TABLE IF EXISTS temp_grade_roster")
        self.conn.commit()
        self.oplog.log_operation(self.op_id, "UPDATE", (admission_number, subject, period), grade, timestamp)
        self.op_id += 1

    def delete(self, admission_number, subject, period):
        timestamp = datetime.now().isoformat()
        with self.conn.cursor() as cursor:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS temp_grade_roster AS
                SELECT admission_number, subject, period, grade, timestamp
                FROM {self.table}
                WHERE NOT (admission_number = '{admission_number}' 
                        AND subject = '{subject}' 
                        AND period = '{period}')
            """)
            cursor.execute(f"""
                INSERT OVERWRITE TABLE {self.table}
                SELECT * FROM temp_grade_roster
            """)
            cursor.execute("DROP TABLE IF EXISTS temp_grade_roster")
        self.conn.commit()
        self.oplog.log_operation(self.op_id, "DELETE", (admission_number, subject, period), timestamp=timestamp)
        self.op_id += 1

    def merge(self, other_system_name):
        oplog_path = OPLOG_PATHS.get(other_system_name.lower())
        if not oplog_path:
            print(f"Invalid system: {other_system_name}")
            return

        other_oplog = OpLogManager(oplog_path)
        operations = other_oplog.read_log()

        for op in operations:
            if op["operation"] in ["INSERT", "UPDATE"] and op["grade"]:
                with self.conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT timestamp FROM {self.table} 
                        WHERE admission_number = '{op["admission_number"]}' 
                        AND subject = '{op["subject"]}' 
                        AND period = '{op["period"]}'
                    """)
                    existing_timestamp = cursor.fetchone()
                    if not existing_timestamp or (existing_timestamp and existing_timestamp[0] < op["timestamp"]):
                        self.update(op["admission_number"], op["subject"], op["period"], op["grade"])
                self.op_id += 1
            elif op["operation"] == "DELETE":
                with self.conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT timestamp FROM {self.table} 
                        WHERE admission_number = '{op["admission_number"]}' 
                        AND subject = '{op["subject"]}' 
                        AND period = '{op["period"]}'
                    """)
                    existing_timestamp = cursor.fetchone()
                    if not existing_timestamp or (existing_timestamp and existing_timestamp[0] <= op["timestamp"]):
                        self.delete(op["admission_number"], op["subject"], op["period"])
                self.op_id += 1