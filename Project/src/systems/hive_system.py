from pyhive import hive
from src.oplog.oplog_manager import OpLogManager
from src.config import HIVE_CONFIG, OPLOG_PATHS
import time
from datetime import datetime
import os
import tempfile

class HiveSystem:
    def __init__(self):
        self.oplog = OpLogManager(OPLOG_PATHS["hive"])
        self.op_id = 1
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.conn = hive.connect(
                    host=HIVE_CONFIG['host'],
                    port=HIVE_CONFIG['port'],
                    database=HIVE_CONFIG['database']
                )
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
                    grade STRING,
                    timestampD STRING
                )
                PARTITIONED BY (admission_number STRING, subject STRING, period STRING)
                ROW FORMAT DELIMITED
                FIELDS TERMINATED BY '\t'
                STORED AS TEXTFILE
            """)

    def _create_temp_data_file(self, grade, timestamp):
        """Create properly formatted data file with tab-separated values"""
        temp_dir = tempfile.gettempdir()
        temp_file = os.path.join(temp_dir, f"hive_data_{self.op_id}.txt")
        with open(temp_file, 'w') as f:
            # Use tab as delimiter between grade and timestamp
            f.write(f"{grade}\t{timestamp}\n")
        return temp_file

    def insert(self, admission_number, subject, period, grade):
        timestamp = datetime.now().isoformat()
        temp_file = self._create_temp_data_file(grade, timestamp)
        
        try:
            with self.conn.cursor() as cursor:
                # Check if record exists
                cursor.execute(f"""
                    SELECT 1 FROM {self.table}
                    WHERE admission_number = '{admission_number}'
                    AND subject = '{subject}'
                    AND period = '{period}'
                    LIMIT 1
                """)
                if not cursor.fetchone():
                    # Create partition if not exists
                    cursor.execute(f"""
                        ALTER TABLE {self.table} ADD IF NOT EXISTS
                        PARTITION (admission_number='{admission_number}', 
                                subject='{subject}', 
                                period='{period}')
                    """)
                    # Load data directly - no need to set delimiter here
                    cursor.execute(f"""
                        LOAD DATA LOCAL INPATH '{temp_file}'
                        INTO TABLE {self.table}
                        PARTITION (admission_number='{admission_number}',
                                subject='{subject}',
                                period='{period}')
                    """)
            self.conn.commit()
            self.oplog.log_operation(
                self.op_id, "INSERT", 
                (admission_number, subject, period), 
                grade, timestamp
            )
            self.op_id += 1
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)

    def read(self, admission_number, subject, period):
        with self.conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT grade FROM {self.table} 
                WHERE admission_number = '{admission_number}' 
                AND subject = '{subject}' 
                AND period = '{period}'
            """)
            result = cursor.fetchone()
        self.oplog.log_operation(
            self.op_id, "READ", 
            (admission_number, subject, period),
            timestamp=datetime.now().isoformat()
        )
        self.op_id += 1
        return result[0] if result else None

    def update(self, admission_number, subject, period, grade):
        timestamp = datetime.now().isoformat()
        temp_file = self._create_temp_data_file(grade, timestamp)
        
        try:
            with self.conn.cursor() as cursor:
                # First delete the existing partition
                cursor.execute(f"""
                    ALTER TABLE {self.table} DROP IF EXISTS
                    PARTITION (
                        admission_number='{admission_number}',
                        subject='{subject}',
                        period='{period}'
                    )
                """)
                # Then add new partition with updated data
                cursor.execute(f"""
                    ALTER TABLE {self.table} ADD
                    PARTITION (
                        admission_number='{admission_number}',
                        subject='{subject}',
                        period='{period}'
                    )
                """)
                cursor.execute(f"""
                    LOAD DATA LOCAL INPATH '{temp_file}'
                    INTO TABLE {self.table}
                    PARTITION (
                        admission_number='{admission_number}',
                        subject='{subject}',
                        period='{period}'
                    )
                """)
            self.conn.commit()
            self.oplog.log_operation(
                self.op_id, "UPDATE",
                (admission_number, subject, period),
                grade, timestamp
            )
            self.op_id += 1
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)

    def delete(self, admission_number, subject, period):
        timestamp = datetime.now().isoformat()
        with self.conn.cursor() as cursor:
            try:
                # First verify the partition exists
                cursor.execute(f"""
                    SHOW PARTITIONS {self.table} 
                    PARTITION(
                        admission_number='{admission_number}', 
                        subject='{subject}', 
                        period='{period}'
                    )
                """)
                partition_exists = cursor.fetchone() is not None
                
                if partition_exists:
                    # Drop the partition
                    cursor.execute(f"""
                        ALTER TABLE {self.table} DROP IF EXISTS
                        PARTITION (
                            admission_number='{admission_number}',
                            subject='{subject}',
                            period='{period}'
                        )
                    """)
                    self.conn.commit()
                    print(f"Successfully deleted partition: {admission_number}/{subject}/{period}")
                else:
                    print(f"Partition not found: {admission_number}/{subject}/{period}")
                    
                self.oplog.log_operation(
                    self.op_id, "DELETE",
                    (admission_number, subject, period),
                    timestamp=timestamp
                )
                self.op_id += 1
                
            except Exception as e:
                print(f"Error deleting partition: {str(e)}")
                self.conn.rollback()
                raise

    def merge(self, other_system_name):
        oplog_path = OPLOG_PATHS.get(other_system_name.lower())
        if not oplog_path:
            print(f"Invalid system: {other_system_name}")
            return

        other_oplog = OpLogManager(oplog_path)
        operations = other_oplog.read_log()

        for op in operations:
            print(op["operation"])
            try:
                ''' if op["operation"] == "INSERT" and op["grade"]:
                    timestamp = datetime.now().isoformat()
                    temp_file = self._create_temp_data_file(op["grade"], timestamp)
                    
                    try:
                        with self.conn.cursor() as cursor:
                            # Check if record exists
                            cursor.execute(f"""
                                SELECT 1 FROM {self.table}
                                WHERE admission_number = '{op["admission_number"]}'
                                AND subject = '{op["subject"]}'
                                AND period = '{op["period"]}'
                                LIMIT 1
                            """)
                            if not cursor.fetchone():
                                # Create partition if not exists
                                cursor.execute(f"""
                                    ALTER TABLE {self.table} ADD IF NOT EXISTS
                                    PARTITION (admission_number='{op["admission_number"]}', 
                                            subject='{op["subject"]}', 
                                            period='{op["period"]}')
                                """)
                                # Load data directly - no need to set delimiter here
                                cursor.execute(f"""
                                    LOAD DATA LOCAL INPATH '{temp_file}'
                                    INTO TABLE {self.table}
                                    PARTITION (admission_number='{op["admission_number"]}',
                                            subject='{op["subject"]}',
                                            period='{op["period"]}')
                                """)
                        self.conn.commit()
                        self.oplog.log_operation(
                            self.op_id, "INSERT", 
                            (op["admission_number"], op["subject"], op["period"]), 
                            op["grade"], timestamp
                        )
                        self.op_id += 1
                    finally:
                        if os.path.exists(temp_file):
                            os.remove(temp_file)    
                elif op["operation"] == "UPDATE" and op["grade"]:
                            timestamp = datetime.now().isoformat()
                            temp_file = self._create_temp_data_file(grade, timestamp)
                            
                            try:
                                with self.conn.cursor() as cursor:
                                    # First delete the existing partition
                                    cursor.execute(f"""
                                        ALTER TABLE {self.table} DROP IF EXISTS
                                        PARTITION (
                                            admission_number='{op["admission_number"]}',
                                            subject='{op["subject"]}',
                                            period='{op["period"]}'
                                        )
                                    """)
                                    # Then add new partition with updated data
                                    cursor.execute(f"""
                                        ALTER TABLE {self.table} ADD
                                        PARTITION (
                                            admission_number='{op["admission_number"]}',
                                            subject='{op["subject"]}',
                                            period='{op["period"]}'
                                        )
                                    """)
                                    cursor.execute(f"""
                                        LOAD DATA LOCAL INPATH '{temp_file}'
                                        INTO TABLE {self.table}
                                        PARTITION (
                                            admission_number='{op["admission_number"]}',
                                            subject='{op["subject"]}',
                                            period='{op["period"]}'
                                        )
                                    """)
                                self.conn.commit()
                                self.oplog.log_operation(
                                    self.op_id, "UPDATE",
                                    (admission_number, subject, period),
                                    grade, timestamp
                                )
                                self.op_id += 1
                            finally:
                                if os.path.exists(temp_file):
                                    os.remove(temp_file)
                '''
                if op["operation"] in ["INSERT", "UPDATE"] and op["grade"]:
                    try:
                        with self.conn.cursor() as cursor:
                            # Get both timestamp and existence info
                            cursor.execute(f"""
                                SELECT   *
                                FROM {self.table} 
                                WHERE admission_number = '{op["admission_number"]}' 
                                AND subject = '{op["subject"]}' 
                                AND period = '{op["period"]}'
                                LIMIT 1
                            """)
                            result = cursor.fetchone()
                            should_update = False
                            should_insert = False
                            if result:
                                grade, existing_timestamp, admission_num, subject, period = result 
                                exists_flag = 1
                                if exists_flag > 0:  # Record exists
                                    if existing_timestamp is None or op["timestamp"] > existing_timestamp or op['grade'] != grade:
                                        should_update = True
                                else:  # New record
                                    should_update = True
                            else:  # No record exists
                                should_insert = True
                                
                            if should_update:
                                print(f"Processing {op['operation']} for {op['admission_number']}")
                                self.update(op["admission_number"], op["subject"], op["period"], op["grade"])
                            elif should_insert:
                                print(f"Processing {op['operation']} for {op['admission_number']}")
                                self.insert(op["admission_number"], op["subject"], op["period"], op["grade"])
                            else:
                                print(f"Skipping {op['operation']} - newer timestamp exists")
                                
                        self.oplog.log_operation(
                            self.op_id, op["operation"],
                            (op["admission_number"], op["subject"], op["period"]),
                            op["grade"], timestamp=datetime.now().isoformat()
                        )
                        self.op_id += 1
                        
                    except Exception as e:
                        print(f"Error processing {op['operation']} merge operation: {str(e)}")
                        continue
                    
                elif op["operation"] == "DELETE":
                    try:
                        with self.conn.cursor() as cursor:
                            # Get both timestamp and existence info
                            cursor.execute(f"""
                                SHOW PARTITIONS {self.table} 
                                PARTITION(
                                    admission_number='{op["admission_number"]}', 
                                    subject='{op["subject"]}', 
                                    period='{op["period"]}'
                                )
                            """)
                            partition_exists = cursor.fetchone() is not None
                            
                            if partition_exists:
                                # Drop the partition
                                cursor.execute(f"""
                                    ALTER TABLE {self.table} DROP IF EXISTS
                                    PARTITION (
                                        admission_number='{op["admission_number"]}',
                                        subject='{op["subject"]}',
                                        period='{op["period"]}'
                                    )
                                """)
                                self.conn.commit()
                                print(f"Successfully deleted partition: {op["admission_number"]}/{op["subject"]}/{op["period"]}")
                            else:
                                print(f"Partition not found: {op["admission_number"]}/{op["subject"]}/{op["period"]}")
           
                        self.oplog.log_operation(
                            self.op_id, "DELETE",
                            (op["admission_number"], op["subject"], op["period"]),
                            timestamp=datetime.now().isoformat()
                        )
                        self.op_id += 1
                        
                    except Exception as e:
                        print(f"Error processing DELETE merge operation: {str(e)}")
                        continue
                    
            except Exception as e:
                print(f"Error processing operation {op}: {str(e)}")
                continue