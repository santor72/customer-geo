from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from .config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB

_engine: Engine | None = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        url = (
            f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
            f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
        )
        _engine = create_engine(url, pool_pre_ping=True)
    return _engine


def get_latest_recv_mon(table_name: str) -> str:
    engine = get_engine()
    sql = text(f"SELECT MAX(recv_mon) AS recv_mon FROM {table_name}")
    with engine.connect() as conn:
        row = conn.execute(sql).mappings().first()
    if not row or not row["recv_mon"]:
        raise ValueError("No data available for current month")
    return row["recv_mon"]
