"""
🚀 High-Performance CSV → PostgreSQL Bulk Loader
Production-safe for UPI / Payment / Banking data
Handles 100M+ rows efficiently using:
  • PostgreSQL COPY (fastest)
  • Chunked streaming (low RAM)
  • Parallel workers (multi-core CPU)
  • Defensive schema inference (TEXT for all identifiers)
"""

import os
import io
import csv
import time
import logging
import argparse
import psycopg2
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     int(os.getenv("PG_PORT", 5432)),
    "dbname":   os.getenv("PG_DB", "rca_agent"),
    "user":     os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASS", "password"),
}

CSV_FILE        = "upi_users_transaction_2026-04-02.csv"
TABLE_NAME      = "upi_transactions"     # <-- REAL TABLE NAME
CHUNK_SIZE      = 100_000
MAX_WORKERS     = 4
DELIMITER       = ","                    # CSV delimiter
CREATE_TABLE    = True
TRUNCATE_FIRST  = False

# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ───────────── Helper: DB Connection ──────────────────

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

# ───────────── Helper: Inference Logic ─────────────────

def infer_pg_type(col_name: str, sample_values: list[str]) -> str:
    """
    Payment-safe schema inference.
    Identifiers are ALWAYS TEXT.
    """
    name = col_name.lower()

    # IDENTIFIER / NON-NUMERIC COLUMNS → ALWAYS TEXT
    if any(k in name for k in (
        "id", "request", "txn", "ref", "merchant",
        "vpa", "lrn", "account", "seq",
        "code", "response", "resp" 
    )):
        return "TEXT"

    clean = [
        v.strip() for v in sample_values
        if v and v.strip() and v.strip().lower() not in ("null", "none")
    ]

    if not clean:
        return "TEXT"

    # integer if in bigint range
    try:
        nums = [int(v) for v in clean[:20]]
        if max(nums) <= 9_223_372_036_854_775_807:
            return "BIGINT"
    except ValueError:
        pass

    # float?
    try:
        [float(v) for v in clean[:20]]
        return "DOUBLE PRECISION"
    except ValueError:
        pass

    # boolean?
    bools = {"true", "false", "yes", "no", "1", "0", "t", "f"}
    if all(v.lower() in bools for v in clean[:20]):
        return "BOOLEAN"

    return "TEXT"

# ───────────── Schema Detection ───────────────────────

def detect_schema(csv_path: str, sample_rows: int = 500) -> dict[str, str]:
    log.info("🔍 Detecting schema from first %d rows...", sample_rows)

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=DELIMITER)
        headers = reader.fieldnames
        samples = {h: [] for h in headers}

        for i, row in enumerate(reader):
            if i >= sample_rows:
                break
            for h in headers:
                samples[h].append(row[h])

    schema = {h: infer_pg_type(h, v) for h, v in samples.items()}
    log.info("✅ Schema detected: %d columns", len(schema))
    return schema

# ───────────── Create Table ─────────────────────────---

def create_table(schema: dict[str, str]):
    cols = ",\n  ".join(f'"{c}" {t}' for c, t in schema.items())
    ddl = f'CREATE TABLE IF NOT EXISTS "{TABLE_NAME}" (\n  {cols}\n);'

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(ddl)
        if TRUNCATE_FIRST:
            cur.execute(f'TRUNCATE TABLE "{TABLE_NAME}"')
        conn.commit()

    log.info("✅ Table '%s' ready.", TABLE_NAME)

# ───────────── COPY Loader (core) ─────────────────────

def copy_chunk(rows, columns) -> int:
    buf = io.StringIO()

    writer = csv.writer(
        buf,
        delimiter=",",
        quoting=csv.QUOTE_ALL,      # CRITICAL FIX: quote all fields
        escapechar="\\"
    )

    for row in rows:
        safe_row = []
        for v in row:
            if v is None or v == "" or str(v).lower() in ("none", "null"):
                safe_row.append("\\N")
            else:
                safe_row.append(str(v))
        writer.writerow(safe_row)

    buf.seek(0)

    col_list = ", ".join(f'"{c}"' for c in columns)

    sql = f'''
        COPY "{TABLE_NAME}" ({col_list})
        FROM STDIN
        WITH (
            FORMAT CSV,
            DELIMITER ',',
            NULL '\\N',
            QUOTE '"',
            ESCAPE '\\'
        )
    '''

    with get_connection() as conn, conn.cursor() as cur:
        cur.copy_expert(sql, buf)
        conn.commit()

    return len(rows)

# ───────────── Main Loader ───────────────────────────

def load_csv(csv_path: str):
    start = time.perf_counter()
    path = Path(csv_path)
    assert path.exists(), f"File not found: {csv_path}"

    log.info("📂 File: %s (%.1f MB)", path.name, path.stat().st_size / (1024**2))

    schema = detect_schema(csv_path)
    if CREATE_TABLE:
        create_table(schema)

    columns = list(schema.keys())
    total = 0
    futures = []

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=DELIMITER, quoting=csv.QUOTE_MINIMAL)
        next(reader)  # skip header

        chunk = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            for row in reader:
                chunk.append(row)

                if len(chunk) >= CHUNK_SIZE:
                    futures.append(pool.submit(copy_chunk, chunk, columns))
                    chunk = []

                done = [f for f in futures if f.done()]
                for d in done:
                    total += d.result()
                    futures.remove(d)

            if chunk:
                futures.append(pool.submit(copy_chunk, chunk, columns))

            for f in as_completed(futures):
                total += f.result()

    elapsed = time.perf_counter() - start
    log.info("─" * 55)
    log.info("✅ Loaded %,d rows in %.1fs (%.0f rows/sec)", total, elapsed, total / elapsed)
    log.info("─" * 55)

# ───────────── CLI ───────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", default=CSV_FILE)
    parser.add_argument("--table", default=TABLE_NAME)
    parser.add_argument("--chunk", type=int, default=CHUNK_SIZE)
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    parser.add_argument("--truncate", action="store_true")
    parser.add_argument("--no-create", action="store_true")
    args = parser.parse_args()

    CSV_FILE        = args.file
    TABLE_NAME      = args.table
    CHUNK_SIZE      = args.chunk
    MAX_WORKERS     = args.workers
    TRUNCATE_FIRST  = args.truncate
    CREATE_TABLE    = not args.no_create

    load_csv(CSV_FILE)