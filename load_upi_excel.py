"""
🚀 High-Performance CSV → PostgreSQL Bulk Loader v5
Uses exact predefined schema for upi_users_transaction table.
"""

import os
import io
import csv
import time
import logging
import argparse
import threading
import psycopg2
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─────────────────────────────────────────────────────────────
# ✅ CONFIG
# ─────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     int(os.getenv("PG_PORT", 5432)),
    "dbname":   os.getenv("PG_DB",   "rca_agent"),
    "user":     os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASS", "123456"),
}

CSV_FILE        = r"D:\OneDrive - Bajaj Finance Limited\Desktop\RCA Agent\upi_users_transaction_2026-04-02.csv"
TABLE_NAME      = "upi_users_transaction"
CHUNK_SIZE      = 50_000
MAX_WORKERS     = 6
MAX_QUEUED      = MAX_WORKERS * 2
DELIMITER       = ","
NULL_VALUES     = {"", "None", "none", "NULL", "null", "NA", "N/A"}
CREATE_TABLE    = True
TRUNCATE_FIRST  = False
BAD_ROWS_LOG    = "bad_rows.csv"
PROGRESS_SEC    = 3

# ─────────────────────────────────────────────────────────────
# ✅ EXACT TABLE SCHEMA  (column order must match CSV header)
# ─────────────────────────────────────────────────────────────
SCHEMA = {
    "trans_id":                             "INTEGER",
    "upi_customer_id":                      "INTEGER",
    "upi_request_id":                       "VARCHAR",
    "payee_merchant_customer_id":           "VARCHAR",
    "payer_merchant_customer_id":           "VARCHAR",
    "device_finger_print":                  "VARCHAR",
    "merchant_request_id":                  "VARCHAR",
    "payee_vpa":                            "VARCHAR",
    "payer_vpa":                            "VARCHAR",
    "payer_name":                           "VARCHAR",
    "payee_name":                           "VARCHAR",
    "amount":                               "DOUBLE PRECISION",
    "remarks":                              "VARCHAR",
    "currency":                             "VARCHAR",
    "bank_account_uniqueid":                "VARCHAR",
    "transaction_type":                     "VARCHAR",
    "flow_type":                            "VARCHAR",
    "type":                                 "VARCHAR",
    "customer_mobile_num":                  "BIGINT",
    "payee_mcc":                            "VARCHAR",
    "status":                               "VARCHAR",
    "gateway_response_code":                "VARCHAR",
    "gateway_response_message":             "VARCHAR",
    "gateway_payee_response_code":          "VARCHAR",
    "gateway_payer_response_code":          "VARCHAR",
    "gateway_payer_reversal_response_code": "VARCHAR",
    "gateway_payee_reversal_response_code": "VARCHAR",
    "ref_url":                              "VARCHAR",
    "ref_category":                         "VARCHAR",
    "transaction_reference":                "VARCHAR",
    "bank_code":                            "INTEGER",
    "masked_account_number":                "VARCHAR",
    "expiry_timestamp":                     "VARCHAR",
    "transaction_timestamp":                "VARCHAR",
    "gateway_reference_id":                 "BIGINT",
    "merchant_channel_id":                  "VARCHAR",
    "merchant_id":                          "VARCHAR",
    "orgmandate_id":                        "VARCHAR",
    "seq_number":                           "INTEGER",
    "umn":                                  "VARCHAR",
    "collect_type":                         "VARCHAR",
    "request_type":                         "VARCHAR",
    "is_verified_payee":                    "BOOLEAN",
    "is_marked_spam":                       "BOOLEAN",
    "query":                                "VARCHAR",
    "query_status":                         "VARCHAR",
    "query_id":                             "VARCHAR",
    "query_date":                           "TIMESTAMP WITHOUT TIME ZONE",
    "remitter_name":                        "VARCHAR",
    "beniname":                             "VARCHAR",
    "benireversarespcode":                  "VARCHAR",
    "remitter_reversal_rescode":            "VARCHAR",
    "disputerc":                            "VARCHAR",
    "dispute_status":                       "VARCHAR",
    "merchant_flag":                        "VARCHAR",
    "tran_desc":                            "VARCHAR",
    "created_date":                         "TIMESTAMP WITHOUT TIME ZONE",
    "updated_date":                         "TIMESTAMP WITHOUT TIME ZONE",
    "created_by":                           "VARCHAR",
    "updated_by":                           "VARCHAR",
    "source":                               "VARCHAR",
    "bank_name":                            "VARCHAR",
    "bank_logo_url":                        "VARCHAR",
    "auto_update_note":                     "VARCHAR",
    "gateway_complain_id":                  "VARCHAR",
    "gateway_complaint_id":                 "VARCHAR",
    "onsdk":                                "BOOLEAN",
    "is_marked_block":                      "VARCHAR",
    "gateway_response_status":              "VARCHAR",
    "base_amount":                          "DOUBLE PRECISION",
    "base_curr":                            "VARCHAR",
    "purpose":                              "INTEGER",
    "fx":                                   "DOUBLE PRECISION",
    "mkup":                                 "DOUBLE PRECISION",
    "account_type":                         "VARCHAR",
    "lrn":                                  "VARCHAR",
    "channel_code":                         "SMALLINT",
    "initiation_mode":                      "VARCHAR",
    "ef_id":                                "VARCHAR",
    "delegatee_vpa":                        "VARCHAR",
    "delegatee_name":                       "VARCHAR",
    "delegatee_mobile_num":                 "VARCHAR",
    "linktype":                             "VARCHAR",
    "cm_flag":                              "BOOLEAN",
    "device_os":                            "VARCHAR",
    "sdk_version":                          "VARCHAR",
}
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ── Atomic counters ───────────────────────────────────────────
_lock           = threading.Lock()
_rows_read      = 0
_rows_committed = 0
_rows_failed    = 0

def _inc_read(n):
    global _rows_read
    with _lock: _rows_read += n

def _inc_committed(n):
    global _rows_committed
    with _lock: _rows_committed += n

def _inc_failed(n):
    global _rows_failed
    with _lock: _rows_failed += n


# ── DB helpers ────────────────────────────────────────────────

def get_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    with conn.cursor() as cur:
        cur.execute("SET synchronous_commit = OFF;")
        cur.execute("SET work_mem = '256MB';")
        cur.execute("SET maintenance_work_mem = '512MB';")
    conn.commit()
    return conn


def is_null(v):
    return v is None or v.strip() in NULL_VALUES


def ensure_table(schema):
    cols_ddl = ",\n  ".join('"' + c + '" ' + t for c, t in schema.items())
    ddl = (
        'CREATE TABLE IF NOT EXISTS "' + TABLE_NAME + '" (\n'
        '  ' + cols_ddl + '\n'
        ');'
    )
    log.info("DDL preview (first 3 cols): %s ...",
             ", ".join(list(schema.keys())[:3]))
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
            if TRUNCATE_FIRST:
                log.warning("Truncating '%s'...", TABLE_NAME)
                cur.execute('TRUNCATE TABLE "' + TABLE_NAME + '";')
        conn.commit()
    finally:
        conn.close()
    log.info("Table '%s' ready — %d columns.", TABLE_NAME, len(schema))


def validate_csv_columns(csv_path, schema):
    """Check CSV header matches schema keys. Warn on mismatches."""
    with open(csv_path, newline="", encoding="utf-8") as f:
        csv_cols = next(csv.reader(f, delimiter=DELIMITER))
    schema_cols = list(schema.keys())

    extra   = [c for c in csv_cols    if c not in schema]
    missing = [c for c in schema_cols if c not in csv_cols]
    if extra:
        log.warning("CSV has %d columns NOT in schema (will be ignored): %s",
                    len(extra), extra)
    if missing:
        log.warning("Schema has %d columns NOT in CSV (will be NULL): %s",
                    len(missing), missing)
    if not extra and not missing:
        log.info("Column validation: OK — all %d columns match.", len(schema_cols))

    # Return ordered column list based on CSV header (skip unknowns)
    return [c for c in csv_cols if c in schema]


# ── Core: COPY chunk ──────────────────────────────────────────

def build_csv_buffer(rows, active_cols, schema):
    """
    Build a CSV buffer for PostgreSQL COPY CSV format.
    NULL  -> empty unquoted field, matched by NULL ''
    csv.writer handles all quoting/escaping automatically.
    """
    buf    = io.StringIO()
    writer = csv.writer(buf, delimiter=",", quoting=csv.QUOTE_MINIMAL)
    for row in rows:
        out = []
        for v, col in zip(row, active_cols):
            if is_null(v):
                out.append(None)   # csv.writer writes None as empty field = NULL
                continue
            v   = v.strip()
            typ = schema.get(col, "VARCHAR")
            if "BOOLEAN" in typ:
                out.append("true" if v.lower() in {"true", "yes", "1", "t"} else "false")
            else:
                out.append(v)      # pass as-is; csv.writer quotes/escapes as needed
        writer.writerow(out)
    return io.BytesIO(buf.getvalue().encode("utf-8"))


def copy_chunk(rows, active_cols, schema, chunk_id):
    buf      = build_csv_buffer(rows, active_cols, schema)
    col_list = ", ".join('"' + c + '"' for c in active_cols)
    # FORMAT CSV + NULL '' => empty unquoted field = NULL (no escape confusion)
    sql      = ('COPY "' + TABLE_NAME + '" (' + col_list + ') '
                "FROM STDIN WITH (FORMAT CSV, DELIMITER ',', NULL '')")
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.copy_expert(sql, buf)
        conn.commit()
        conn.close()
        _inc_committed(len(rows))
        return len(rows), 0
    except Exception as e:
        log.error("Chunk %d COPY failed (%d rows): %s", chunk_id, len(rows), e)
        try:
            if conn: conn.rollback(); conn.close()
        except Exception: pass
        return _insert_row_by_row(rows, active_cols, schema, chunk_id)


def _insert_row_by_row(rows, active_cols, schema, chunk_id):
    loaded = failed = 0
    col_list = ", ".join('"' + c + '"' for c in active_cols)
    ph  = ", ".join(["%s"] * len(active_cols))
    sql = ('INSERT INTO "' + TABLE_NAME + '" (' + col_list +
           ') VALUES (' + ph + ') ON CONFLICT DO NOTHING;')
    bad_rows = []
    conn = get_connection()
    for row in rows:
        vals = []
        for v, col in zip(row, active_cols):
            typ = schema.get(col, "VARCHAR")
            if is_null(v):
                vals.append(None)
            elif "BOOLEAN" in typ:
                vals.append(v.lower() in {"true", "yes", "1", "t"})
            elif typ in ("INTEGER", "SMALLINT"):
                try: vals.append(int(float(v.strip())))
                except ValueError: vals.append(None)
            elif typ == "BIGINT":
                try: vals.append(int(v.strip()))
                except ValueError: vals.append(None)
            elif "DOUBLE" in typ:
                try: vals.append(float(v.strip()))
                except ValueError: vals.append(None)
            else:
                vals.append(v.strip())
        try:
            with conn.cursor() as cur:
                cur.execute(sql, vals)
            conn.commit()
            loaded += 1
        except Exception:
            conn.rollback()
            bad_rows.append(row)
            failed += 1
    conn.close()
    if bad_rows:
        with open(BAD_ROWS_LOG, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(bad_rows)
        log.warning("Chunk %d: %d bad rows → %s", chunk_id, len(bad_rows), BAD_ROWS_LOG)
    _inc_committed(loaded)
    _inc_failed(failed)
    return loaded, failed


def _wrapped_copy(rows, active_cols, schema, chunk_id, sem):
    try:
        return copy_chunk(rows, active_cols, schema, chunk_id)
    finally:
        sem.release()


# ── Background progress thread ────────────────────────────────

def _progress_worker(start, total_estimate, stop_event):
    while not stop_event.is_set():
        time.sleep(PROGRESS_SEC)
        elapsed     = time.perf_counter() - start
        read        = _rows_read
        committed   = _rows_committed
        read_rate   = read / elapsed if elapsed > 0 else 0
        commit_rate = committed / elapsed if elapsed > 0 else 0
        pct         = (read / total_estimate * 100) if total_estimate > 0 else 0
        eta_min     = ((total_estimate - read) / read_rate / 60) if read_rate > 0 else 0
        log.info(
            "  [READ %s/%s %.1f%%]  read: %s r/s  write: %s r/s  committed: %s  ETA: %.1f min",
            f"{read:,}", f"{total_estimate:,}", pct,
            f"{int(read_rate):,}", f"{int(commit_rate):,}",
            f"{committed:,}", eta_min,
        )


# ── Row count ─────────────────────────────────────────────────

def count_lines_fast(path):
    log.info("Counting rows (fast scan)...")
    count = 0
    with open(path, "rb") as f:
        for buf in iter(lambda: f.read(8 * 1024 * 1024), b""):
            count += buf.count(b"\n")
    return count - 1


# ── Main ──────────────────────────────────────────────────────

def load_csv(csv_path):
    global _rows_read, _rows_committed, _rows_failed
    _rows_read = _rows_committed = _rows_failed = 0

    start = time.perf_counter()
    path  = Path(csv_path)
    assert path.exists(), f"File not found: {csv_path}"

    mb = path.stat().st_size / (1024 ** 2)
    log.info("File  : %s", path.name)
    log.info("Size  : %.1f MB (%.2f GB)", mb, mb / 1024)

    total_estimate = count_lines_fast(csv_path)
    log.info("Rows  : ~%s", f"{total_estimate:,}")

    # Validate + get ordered active columns from CSV header
    active_cols = validate_csv_columns(csv_path, SCHEMA)
    log.info("Active columns: %d", len(active_cols))

    if CREATE_TABLE:
        ensure_table(SCHEMA)

    log.info("Workers: %d  |  Chunk: %s rows  |  Max queued: %d",
             MAX_WORKERS, f"{CHUNK_SIZE:,}", MAX_QUEUED)
    log.info("-" * 68)
    log.info("Starting load...")

    stop_event  = threading.Event()
    prog_thread = threading.Thread(
        target=_progress_worker, args=(start, total_estimate, stop_event), daemon=True
    )
    prog_thread.start()

    sem      = threading.Semaphore(MAX_QUEUED)
    futures  = []
    chunk_id = 0

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=DELIMITER)
        header = next(reader)
        # Build index map: position in CSV row → only columns in schema
        col_indices = [i for i, c in enumerate(header) if c in SCHEMA]

        chunk = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            for row in reader:
                # Extract only the columns we care about, in order
                filtered = [row[i] for i in col_indices]
                chunk.append(filtered)
                if len(chunk) >= CHUNK_SIZE:
                    _inc_read(len(chunk))
                    sem.acquire()
                    futures.append(
                        pool.submit(_wrapped_copy, chunk, active_cols, SCHEMA, chunk_id, sem)
                    )
                    chunk_id += 1
                    chunk = []

            if chunk:
                _inc_read(len(chunk))
                sem.acquire()
                futures.append(
                    pool.submit(_wrapped_copy, chunk, active_cols, SCHEMA, chunk_id, sem)
                )

            for fut in as_completed(futures):
                pass

    stop_event.set()
    prog_thread.join(timeout=2)

    elapsed = time.perf_counter() - start
    rate    = _rows_committed / elapsed if elapsed > 0 else 0
    log.info("-" * 68)
    log.info("DONE!")
    log.info("  Committed : %s rows", f"{_rows_committed:,}")
    log.info("  Failed    : %s rows", f"{_rows_failed:,}")
    log.info("  Time      : %.1f sec  (%.1f min)", elapsed, elapsed / 60)
    log.info("  Avg speed : %s rows/sec", f"{int(rate):,}")
    if _rows_failed > 0:
        log.warning("  Bad rows  : saved to %s", BAD_ROWS_LOG)
    log.info("-" * 68)


# ── CLI ───────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fast CSV to PostgreSQL loader v5")
    parser.add_argument("--file",      default=CSV_FILE,   help="CSV file path")
    parser.add_argument("--table",     default=TABLE_NAME, help="Target table")
    parser.add_argument("--chunk",     type=int, default=CHUNK_SIZE,  help="Rows per batch")
    parser.add_argument("--workers",   type=int, default=MAX_WORKERS, help="Parallel workers")
    parser.add_argument("--truncate",  action="store_true", help="Truncate before load")
    parser.add_argument("--no-create", action="store_true", help="Skip table creation")
    parser.add_argument("--no-count",  action="store_true", help="Skip row count scan")
    args = parser.parse_args()

    CSV_FILE       = args.file
    TABLE_NAME     = args.table
    CHUNK_SIZE     = args.chunk
    MAX_WORKERS    = args.workers
    TRUNCATE_FIRST = args.truncate
    CREATE_TABLE   = not args.no_create

    if args.no_count:
        def count_lines_fast(_): return 0

    load_csv(CSV_FILE)
 