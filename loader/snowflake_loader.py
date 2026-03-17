import logging
from contextlib import contextmanager

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from config import settings

logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS BOOKETL_DB.RAW.RAW_BOOKS (
    TITLE            VARCHAR(500),
    PRICE            FLOAT,
    RATING           INTEGER,
    IN_STOCK         BOOLEAN,
    CATEGORY         VARCHAR(200),
    URL              VARCHAR(1000),
    PRICE_BAND       VARCHAR(50),
    IS_HIGHLY_RATED  BOOLEAN,
    SCRAPED_AT       VARCHAR(50)
)
"""

TRUNCATE_SQL = "TRUNCATE TABLE IF EXISTS BOOKETL_DB.RAW.RAW_BOOKS"


@contextmanager
def get_conn():
    conn = snowflake.connector.connect(**settings.snowflake_conn)
    try:
        yield conn
    finally:
        conn.close()


def load(df: pd.DataFrame) -> int:
    if df.empty:
        logger.warning("Empty DataFrame — nothing to load.")
        return 0

    df_load = df.copy()
    df_load.columns = [c.upper() for c in df_load.columns]

    # Convert booleans to strings for Snowflake compatibility
    for col in df_load.columns:
        if df_load[col].dtype == bool:
            df_load[col] = df_load[col].astype(str)

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(CREATE_TABLE_SQL)
        cur.execute(TRUNCATE_SQL)
        logger.info("Loading %d rows into Snowflake...", len(df_load))

        # Build INSERT statements in batches of 100
        cols = ", ".join(df_load.columns)
        rows_loaded = 0
        batch_size = 100

        for i in range(0, len(df_load), batch_size):
            batch = df_load.iloc[i:i + batch_size]
            values = []
            for _, row in batch.iterrows():
                escaped = []
                for val in row:
                    if val is None or (isinstance(val, float) and pd.isna(val)):
                        escaped.append("NULL")
                    else:
                        escaped.append("'" + str(val).replace("'", "''") + "'")
                values.append("(" + ", ".join(escaped) + ")")

            sql = f"INSERT INTO BOOKETL_DB.RAW.RAW_BOOKS ({cols}) VALUES {', '.join(values)}"
            cur.execute(sql)
            rows_loaded += len(batch)

        conn.commit()

    logger.info("Loaded %d rows into BOOKETL_DB.RAW.RAW_BOOKS", rows_loaded)
    return rows_loaded
