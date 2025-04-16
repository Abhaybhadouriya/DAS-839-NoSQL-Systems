import os
from src.config import HIVE_CONFIG, MONGO_CONFIG, MYSQL_CONFIG, OPLOG_PATHS
from pymongo import MongoClient
from mysql.connector import connect, Error
from pyhive import hive

def drop_hive_tables():
    try:
        conn = hive.connect(host=HIVE_CONFIG['host'], port=HIVE_CONFIG['port'], database=HIVE_CONFIG['database'])
        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            for table in tables:
                cursor.execute(f"DROP TABLE IF EXISTS {table[0]}")
        conn.commit()
        print(f"Dropped all tables in Hive database {HIVE_CONFIG['database']}")
    except Exception as e:
        print(f"Error dropping Hive tables: {e}")
    finally:
        conn.close()

def drop_mongo_tables():
    try:
        client = MongoClient(host=MONGO_CONFIG['host'], port=MONGO_CONFIG['port'])
        db = client[MONGO_CONFIG['database']]
        db.drop_collection(MONGO_CONFIG['collection'])
        print(f"Dropped collection {MONGO_CONFIG['collection']} in MongoDB database {MONGO_CONFIG['database']}")
    except Exception as e:
        print(f"Error dropping MongoDB collection: {e}")
    finally:
        client.close()

def drop_mysql_tables():
    try:
        conn = connect(
            host=MYSQL_CONFIG['host'],
            port=MYSQL_CONFIG['port'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password'],
            database=MYSQL_CONFIG['database']
        )
        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            for table in tables:
                cursor.execute(f"DROP TABLE IF EXISTS {table[0]}")
        conn.commit()
        print(f"Dropped all tables in MySQL database {MYSQL_CONFIG['database']}")
    except Error as e:
        print(f"Error dropping MySQL tables: {e}")
    finally:
        conn.close()

def delete_logs():
    try:
        for log_path in OPLOG_PATHS.values():
            if os.path.exists(log_path):
                os.remove(log_path)
                print(f"Deleted log file: {log_path}")
            else:
                print(f"Log file not found: {log_path}")
    except Exception as e:
        print(f"Error deleting logs: {e}")

def main():
    drop_hive_tables()
    drop_mongo_tables()
    drop_mysql_tables()
    delete_logs()
    print("Cleanup completed for all databases and logs.")

if __name__ == "__main__":
    main()