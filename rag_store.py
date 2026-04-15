import psycopg2
import logging
import uuid
from datetime import datetime
from typing import Dict, Any
import os

from embeddings import embed  # Use your working embedding function

logger = logging.getLogger("rag-store")

# -----------------------------------------
# DB CONFIG (update from env if needed)
# -----------------------------------------
DB_CONN = {
    "dbname": os.getenv("RAG_DB_NAME", "rca_agent"),
    "user": os.getenv("RAG_DB_USER", "postgres"),
    "password": os.getenv("RAG_DB_PASSWORD", "postgres"),
    "host": os.getenv("RAG_DB_HOST", "localhost"),
    "port": os.getenv("RAG_DB_PORT", "5432"),
}

# -----------------------------------------
# STORE RCA MEMORY INTO PGVECTOR
# -----------------------------------------
def store_rca_memory(
    alert_type: str,
    facts: Dict[str, Any],
    rca: Dict[str, Any],
    min_confidence: float = 0.70,
):
    """
    Store high-confidence RCA into pgvector memory.
    - Never breaks RCA pipeline
    - Stores only good-quality RCA
    - Embeds content using working MiniLM model
    """

    try:
        # Extract confidence
        confidence_value = float(rca.get("confidence", 0))

        # Skip low-confidence RCAs
        if confidence_value < min_confidence:
            logger.info(f"[STORE] Skipping (confidence={confidence_value})")
            return

        # -------------------------------------
        # Build textual memory "content"
        # -------------------------------------
        content = f"""
Alert Type: {alert_type}

Root Cause:
{rca.get("root_cause")}

Why:
{rca.get("why")}

Evidence:
{rca.get("evidence")}

Timeline:
{rca.get("timeline")}

Impact:
{rca.get("impact")}

Recommended Actions:
{rca.get("recommended_actions")}

Facts:
{facts}
""".strip()

        # -------------------------------------
        # Generate embedding
        # -------------------------------------
        vector = embed(content)

        if not vector:
            logger.error("[STORE] Embedding returned None, skipping store")
            return

        # -------------------------------------
        # Insert into PostgreSQL pgvector table
        # -------------------------------------
        with psycopg2.connect(**DB_CONN) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO rca_knowledge
                    (id, content, embedding, created_at)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (str(uuid.uuid4()), content, vector, datetime.utcnow())
                )
                conn.commit()

        logger.info("[STORE] RCA memory stored successfully.")

    except Exception as e:
        logger.error("[STORE] Failed to store RCA memory", exc_info=e)