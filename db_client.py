import psycopg2
import logging
import os
from typing import Tuple, Optional

logger = logging.getLogger("db-client")

# ================================
# DATABASE CONFIGURATION (ENV-DRIVEN)
# ================================
DB_CONN = {
    "dbname": os.getenv("DB_NAME", "rca_agent"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "your_pass"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

# ================================
# BUSINESS IMPACT QUERY (RCA CORE)
# ================================
def get_top_failure(window_minutes: int = 5) -> Tuple[Optional[str], Optional[str], int]:
    """
    Returns:
      (bank_code, error_code, failure_count)

    Safe for production:
    - Never crashes caller
    - Always returns a tuple
    - Degrades gracefully if DB is unreachable
    """

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONN)
        cur = conn.cursor()

        cur.execute(
            """
            SELECT bank_code, gateway_response_code, COUNT(*)
            FROM upi_users_transaction
            WHERE created_date > NOW() - INTERVAL %s
              AND status = 'FAILED'
            GROUP BY bank_code, gateway_response_code
            ORDER BY COUNT(*) DESC
            LIMIT 1
            """,
            (f"{window_minutes} minutes",)
        )

        result = cur.fetchone()
        cur.close()

        if not result:
            return None, None, 0

        bank, error_code, count = result
        return bank, error_code, int(count)

    except Exception as e:
        logger.error("Postgres query failed in get_top_failure()", exc_info=e)
        return None, None, 0

    finally:
        if conn:
            conn.close()