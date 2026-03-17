"""
One-time setup — run this before pipeline.py for the first time.
Creates the RAW_BOOKS table in Snowflake.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from loader.snowflake_loader import get_conn, CREATE_TABLE_SQL

if __name__ == "__main__":
    with get_conn() as conn:
        conn.cursor().execute(CREATE_TABLE_SQL)
    print("Snowflake table created successfully.")
