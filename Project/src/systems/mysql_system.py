# import mysql.connector
# from src.oplog.oplog_manager import OpLogManager
# from src.config import MYSQL_CONFIG, OPLOG_PATHS
# import time
# from datetime import datetime

# class MySQLSystem:
#     def __init__(self):
#         self.oplog = OpLogManager(OPLOG_PATHS["sql"])
#         self.op_id = 1
#         max_retries = 3
#         for attempt in range(max_retries):
#             try:
#                 self.conn = mysql.connector.connect(
#                     host=MYSQL_CONFIG['host'],
#                     port=MYSQL_CONFIG['port'],
#                     user=MYSQL_CONFIG['user'],
#                     password=MYSQL_CONFIG['password'],
#                     database=MYSQL_CONFIG['database']
#                 )
#                 break
#             except mysql.connector.Error as e:
#                 if attempt < max_retries - 1:
#                     time.sleep(2 ** attempt)
#                     continue
#                 raise Exception(f"Failed to connect to MySQL after {max_retries} attempts: {e}")
#         self.table = MYSQL_CONFIG['table']
#         with self.conn.cursor() as cursor:
#             cursor.execute(f"""
#                 CREATE TABLE IF NOT EXISTS {self.table} (
#                     admission_number VARCHAR(255),
#                     subject VARCHAR(255),
#                     period VARCHAR(255),
#                     grade VARCHAR(10),
#                     timestamp VARCHAR(50),
#                     PRIMARY KEY (admission_number, subject, period)
#                 )
#             """)
#         self.conn.commit()

#     def insert(self, admission_number, subject, period, grade):
#         timestamp = datetime.now().isoformat()
#         with self.conn.cursor() as cursor:
#             cursor.execute(f"""
#                 INSERT INTO {self.table} (admission_number, subject, period, grade, timestamp)
#                 VALUES (%s, %s, %s, %s, %s)
#                 ON DUPLICATE KEY UPDATE grade = %s, timestamp = %s
#             """, (admission_number, subject, period, grade, timestamp, grade, timestamp))
#         self.conn.commit()
#         self.oplog.log_operation(self.op_id, "INSERT", (admission_number, subject, period), grade, timestamp)
#         self.op_id += 1

#     def read(self, admission_number, subject, period):
#         with self.conn.cursor() as cursor:
#             cursor.execute(f"""
#                 SELECT grade FROM {self.table} 
#                 WHERE admission_number = %s AND subject = %s AND period = %s
#             """, (admission_number, subject, period))
#             result = cursor.fetchone()
#         self.oplog.log_operation(self.op_id, "READ", (admission_number, subject, period), timestamp=datetime.now().isoformat())
#         self.op_id += 1
#         return result[0] if result else None

#     def update(self, admission_number, subject, period, grade):
#         timestamp = datetime.now().isoformat()
#         with self.conn.cursor() as cursor:
         
#             cursor.execute(f"""
#                 UPDATE {self.table} 
#                 SET grade = %s, timestamp = %s 
#                 WHERE admission_number = %s AND subject = %s AND period = %s
#             """, (grade, timestamp, admission_number, subject, period))

#         self.conn.commit()
#         self.oplog.log_operation(self.op_id, "UPDATE", (admission_number, subject, period), grade, timestamp)
#         self.op_id += 1

#     def delete(self, admission_number, subject, period):
#         timestamp = datetime.now().isoformat()
#         with self.conn.cursor() as cursor:
#             cursor.execute(f"""
#                 DELETE FROM {self.table} 
#                 WHERE admission_number = %s AND subject = %s AND period = %s
#             """, (admission_number, subject, period))
#         self.conn.commit()
#         self.oplog.log_operation(self.op_id, "DELETE", (admission_number, subject, period), timestamp=timestamp)
#         self.op_id += 1

#     def merge(self, other_system_name):
#         oplog_path = OPLOG_PATHS.get(other_system_name.lower())
#         if not oplog_path:
#             print(f"Invalid system SQL_Merge: {other_system_name}")
#             return

#         other_oplog = OpLogManager(oplog_path)
#         operations = other_oplog.read_log()

#         for op in operations:
#             print(op)
#             if op["operation"] in ["INSERT", "UPDATE"] and op["grade"]:
#                 with self.conn.cursor() as cursor:
#                     cursor.execute(f"""
#                         SELECT timestamp FROM {self.table} 
#                         WHERE admission_number = %s AND subject = %s AND period = %s
#                     """, (op["admission_number"], op["subject"], op["period"]))
#                     existing_timestamp = cursor.fetchone()
                    
#                     # Compare only if both timestamps are valid
#                     if not existing_timestamp or (existing_timestamp[0] and existing_timestamp[0] < op["timestamp"]):
#                         cursor.execute(f"""
#                             INSERT INTO {self.table} (admission_number, subject, period, grade, timestamp)
#                             VALUES (%s, %s, %s, %s, %s)
#                             ON DUPLICATE KEY UPDATE grade = %s, timestamp = %s
#                         """, (op["admission_number"], op["subject"], op["period"], op["grade"], op["timestamp"], op["grade"], op["timestamp"]))
#                 self.conn.commit()
#                 self.oplog.log_operation(self.op_id, op["operation"], (op["admission_number"], op["subject"], op["period"]), op["grade"], timestamp=datetime.now().isoformat())
#                 self.op_id += 1

#             elif op["operation"] == "DELETE":
#                 with self.conn.cursor() as cursor:
#                     cursor.execute(f"""
#                         SELECT timestamp FROM {self.table} 
#                         WHERE admission_number = %s AND subject = %s AND period = %s
#                     """, (op["admission_number"], op["subject"], op["period"]))
#                     existing_timestamp = cursor.fetchone()
                   
#                     if not existing_timestamp or (existing_timestamp[0] and existing_timestamp[0] < op["timestamp"]):
#                         cursor.execute(f"""
#                             DELETE FROM {self.table} 
#                             WHERE admission_number = %s AND subject = %s AND period = %s
#                         """, (op["admission_number"], op["subject"], op["period"]))
#                     else:
#                         print(f"Skipping DELETE operation for {op['admission_number']}, {op['timestamp']}, {existing_timestamp[0]} due to older timestamp.")
#                 self.conn.commit()
#                 self.oplog.log_operation(self.op_id, "DELETE", (op["admission_number"], op["subject"], op["period"]), timestamp=datetime.now().isoformat())
#                 self.op_id += 1




import mysql.connector
from src.oplog.oplog_manager import OpLogManager
from src.config import MYSQL_CONFIG, OPLOG_PATHS
import time
from datetime import datetime

class MySQLSystem:
    def __init__(self):
        self.oplog = OpLogManager(OPLOG_PATHS["sql"])
        self.op_id = 1
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.conn = mysql.connector.connect(
                    host=MYSQL_CONFIG['host'],
                    port=MYSQL_CONFIG['port'],
                    user=MYSQL_CONFIG['user'],
                    password=MYSQL_CONFIG['password'],
                    database=MYSQL_CONFIG['database']
                )
                break
            except mysql.connector.Error as e:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                raise Exception(f"Failed to connect to MySQL after {max_retries} attempts: {e}")
        self.table = MYSQL_CONFIG['table']
        with self.conn.cursor() as cursor:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table} (
                    sid VARCHAR(255),
                    course VARCHAR(255),
                    grade VARCHAR(10),
                    timestamp VARCHAR(50),
                    PRIMARY KEY (sid, course)
                )
            """)
        self.conn.commit()

    def insert(self, sid, course, grade):
        timestamp = datetime.now().isoformat()
        with self.conn.cursor() as cursor:
            cursor.execute(f"""
                INSERT INTO {self.table} (sid, course, grade, timestamp)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE grade = %s, timestamp = %s
            """, (sid, course, grade, timestamp, grade, timestamp))
        self.conn.commit()
        self.oplog.log_operation(self.op_id, "SET", (sid, course), grade, timestamp)
        self.op_id += 1

    def read(self, sid, course):
        with self.conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT grade FROM {self.table} 
                WHERE sid = %s AND course = %s
            """, (sid, course))
            result = cursor.fetchone()
        self.oplog.log_operation(self.op_id, "GET", (sid, course), timestamp=datetime.now().isoformat())
        self.op_id += 1
        return result[0] if result else None

    def update(self, sid, course, grade):
        timestamp = datetime.now().isoformat()
        with self.conn.cursor() as cursor:
            cursor.execute(f"""
                UPDATE {self.table} 
                SET grade = %s, timestamp = %s 
                WHERE sid = %s AND course = %s
            """, (grade, timestamp, sid, course))

        self.conn.commit()
        self.oplog.log_operation(self.op_id, "SET", (sid, course), grade, timestamp)
        self.op_id += 1

    def delete(self, sid, course):
        timestamp = datetime.now().isoformat()
        with self.conn.cursor() as cursor:
            cursor.execute(f"""
                DELETE FROM {self.table} 
                WHERE sid = %s AND course = %s
            """, (sid, course))
        self.conn.commit()
        self.oplog.log_operation(self.op_id, "DELETE", (sid, course), timestamp=timestamp)
        self.op_id += 1

    def merge(self, other_system_name):
        oplog_path = OPLOG_PATHS.get(other_system_name.lower())
        if not oplog_path:
            print(f"Invalid system SQL_Merge: {other_system_name}")
            return

        other_oplog = OpLogManager(oplog_path)
        operations = other_oplog.read_log()

        for op in operations:
            print(op)
            if op["operation"] in ["SET"] and op["grade"]:
                with self.conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT timestamp FROM {self.table} 
                        WHERE sid = %s AND course = %s
                    """, (op["sid"], op["course"]))
                    existing_timestamp = cursor.fetchone()
                    
                    # Compare only if both timestamps are valid
                    if not existing_timestamp or (existing_timestamp[0] and existing_timestamp[0] < op["timestamp"]):
                        cursor.execute(f"""
                            INSERT INTO {self.table} (sid, course, grade, timestamp)
                            VALUES (%s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE grade = %s, timestamp = %s
                        """, (op["sid"], op["course"], op["grade"], op["timestamp"], op["grade"], op["timestamp"]))
                self.conn.commit()
                # self.oplog.log_operation(self.op_id, op["operation"], (op["sid"], op["course"]), op["grade"], timestamp=datetime.now().isoformat())
                self.oplog.log_operation(self.op_id, op["operation"], (op["sid"], op["course"]), op["grade"], op["timestamp"])
                self.op_id += 1

            elif op["operation"] == "DELETE":
                with self.conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT timestamp FROM {self.table} 
                        WHERE sid = %s AND course = %s
                    """, (op["sid"], op["course"]))
                    existing_timestamp = cursor.fetchone()
                   
                    if not existing_timestamp or (existing_timestamp[0] and existing_timestamp[0] < op["timestamp"]):
                        cursor.execute(f"""
                            DELETE FROM {self.table} 
                            WHERE sid = %s AND course = %s
                        """, (op["sid"], op["course"]))
                    else:
                        print(f"Skipping DELETE operation for {op['sid']}, {op['timestamp']}, {existing_timestamp[0]} due to older timestamp.")
                self.conn.commit()
                # self.oplog.log_operation(self.op_id, "DELETE", (op["sid"], op["course"]), timestamp=datetime.now().isoformat())
                self.oplog.log_operation(self.op_id, "DELETE", (op["sid"], op["course"]), op["timestamp"])

                self.op_id += 1