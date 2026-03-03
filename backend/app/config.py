import os
from dotenv import load_dotenv

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "schema_customer")
MYSQL_SCHEMA = os.getenv("MYSQL_SCHEMA", "")

DADATA_TOKEN = os.getenv("DADATA_TOKEN", "") or os.getenv("DADATA_SECRET", "")
DADATA_SECRET = os.getenv("DADATA_SECRET", "")
YANDEX_API_KEY = os.getenv("YANDEX_API_KEY", "")
