import os
import logging
from typing import Dict, Optional

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table

logger = logging.getLogger("adx-client")

# ================================
# CONFIG (ENV-DRIVEN)
# ================================
ADX_CLUSTER = os.getenv(
    "ADX_CLUSTER",
    "https://bpay-app-logs-pr-adx.centralindia.kusto.windows.net"
)
ADX_DATABASE = os.getenv(
    "ADX_DATABASE",
    "bpay-app-logs-pr-adx-db"
)

ADX_CLIENT_ID = os.getenv("ADX_CLIENT_ID")
ADX_CLIENT_SECRET = os.getenv("ADX_CLIENT_SECRET")
ADX_TENANT_ID = os.getenv("ADX_TENANT_ID")

# ================================
# LAZY SINGLETON CLIENT
# ================================
_client: Optional[KustoClient] = None


def _get_client() -> Optional[KustoClient]:
    """
    Lazily initialize ADX client.
    Never crashes service if ADX is unavailable.
    """
    global _client

    if _client:
        return _client

    if not ADX_CLIENT_ID or not ADX_CLIENT_SECRET or not ADX_TENANT_ID:
        logger.error("ADX credentials missing; infra facts will be skipped")
        return None

    try:
        _client = KustoClient(
            KustoConnectionStringBuilder.with_aad_application_key_authentication(
                ADX_CLUSTER,
                ADX_CLIENT_ID,
                ADX_CLIENT_SECRET,
                ADX_TENANT_ID
            )
        )
        logger.info("ADX client initialized successfully")
        return _client

    except Exception as e:
        logger.error("Failed to initialize ADX client", exc_info=e)
        return None


# ================================
# 1️⃣ LATENCY SIGNAL (FAST, AGGREGATED)
# ================================
def get_latency(minutes: int = 5) -> float:
    """
    Returns average latency (ms) for the given window.
    Always returns a number.
    Safe fallback: 0.0
    """

    client = _get_client()
    if not client:
        return 0.0

    query = f"""
    union upi_logs_set_1_prod,
          upi_logs_set_2_prod,
          upi_logs_set_3_prod,
          upi_logs_set_4_prod,
          upi_logs_set_5_prod
    | where created_date > ago({minutes}m)
    | summarize avg_latency = avg(DurationMs)
    """

    try:
        response = client.execute(ADX_DATABASE, query)
        table = response.primary_results[0]

        if not table or table[0]["avg_latency"] is None:
            return 0.0

        return float(table[0]["avg_latency"])

    except KustoServiceError as e:
        logger.error("ADX latency query failed", exc_info=e)
        return 0.0
    except Exception as e:
        logger.error("Unexpected ADX latency error", exc_info=e)
        return 0.0


# ================================
# 2️⃣ ERROR SUMMARY (LLM-SAFE)
# ================================
def get_recent_error_summary(minutes: int = 30) -> Dict[str, Optional[int]]:
    """
    Returns aggregated error metadata for RCA reasoning.

    Safe fallback on any failure.
    """

    client = _get_client()
    if not client:
        return {
            "total_error_logs": 0,
            "top_error_message": None,
            "top_error_count": 0,
        }

    query = f"""
    union upi_logs_set_1_prod,
          upi_logs_set_2_prod,
          upi_logs_set_3_prod,
          upi_logs_set_4_prod,
          upi_logs_set_5_prod
    | where created_date > ago({minutes}m)
    | where level == "ERROR"
    | summarize error_count = count() by message
    | order by error_count desc
    """

    try:
        response = client.execute(ADX_DATABASE, query)
        df = dataframe_from_result_table(response.primary_results[0])

        if df.empty:
            return {
                "total_error_logs": 0,
                "top_error_message": None,
                "top_error_count": 0,
            }

        top_row = df.iloc[0]
        return {
            "total_error_logs": int(df["error_count"].sum()),
            "top_error_message": top_row["message"],
            "top_error_count": int(top_row["error_count"]),
        }

    except KustoServiceError as e:
        logger.error("ADX error summary failed", exc_info=e)
        return {
            "total_error_logs": 0,
            "top_error_message": None,
            "top_error_count": 0,
        }
    except Exception as e:
        logger.error("Unexpected ADX error summary failure", exc_info=e)
        return {
            "total_error_logs": 0,
            "top_error_message": None,
            "top_error_count": 0,
        }


# ================================
# 3️⃣ RAW ERROR LOGS (DEBUG ONLY)
# ================================
def get_recent_error_logs_df(minutes: int = 30):
    """
    DEBUG / MANUAL USE ONLY.
    DO NOT use this in automated RCA logic.
    """

    client = _get_client()
    if not client:
        return None

    query = f"""
    union upi_logs_set_1_prod,
          upi_logs_set_2_prod,
          upi_logs_set_3_prod,
          upi_logs_set_4_prod,
          upi_logs_set_5_prod
    | where created_date > ago({minutes}m)
    | where level == "ERROR"
    | project created_date, appname, message
    | limit 100
    """

    try:
        response = client.execute(ADX_DATABASE, query)
        return dataframe_from_result_table(response.primary_results[0])
    except Exception as e:
        logger.error("ADX raw log query failed", exc_info=e)
        return None