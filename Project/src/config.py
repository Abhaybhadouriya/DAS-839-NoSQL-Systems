import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

MONGO_CONFIG = {
    'host': os.environ.get('MONGO_HOST', 'localhost'),
    'port': int(os.environ.get('MONGO_PORT', 27017)),
    'database': os.environ.get('MONGO_DB', 'grade_db'),
    'collection': os.environ.get('MONGO_COLLECTION', 'grade_roster')
}

HIVE_CONFIG = {
    'host': os.environ.get('HIVE_HOST', 'localhost'),
    'port': int(os.environ.get('HIVE_PORT', 10000)),
    'database': os.environ.get('HIVE_DB', 'default')
}

MYSQL_CONFIG = {
    'host': os.environ.get('MYSQL_HOST', 'localhost'),
    'port': int(os.environ.get('MYSQL_PORT', 3306)),
    'user': os.environ.get('MYSQL_USER', 'root'),
    'password': os.environ.get('MYSQL_PASSWORD', ''),
    'database': os.environ.get('MYSQL_DB', 'grade_db')
}

OPLOG_PATHS = {
    "mongo": "../logs/oplog_mongo.log",
    "hive": "../logs/oplog_hive.log",
    "mysql": "../logs/oplog_mysql.log"
}