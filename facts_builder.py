from db_client import get_top_failure
from adx_client import get_latency, get_recent_error_summary


def build_facts(alert_type: str):

    try:
        bank, error_code, failure_count = get_top_failure()
        latency = get_latency()
        adx = get_recent_error_summary(30)

        return {
            "alert_type": alert_type,

            "bank": bank,
            "error_code": error_code,
            "failure_count": failure_count,

            "latency_ms": latency,
            "latency_spike": latency > 2000,

            "adx_total_errors": adx["total_error_logs"],
            "adx_top_error_message": adx["top_error_message"],
            "adx_top_error_count": adx["top_error_count"],
        }

    except Exception as e:
        return {"error": str(e)}