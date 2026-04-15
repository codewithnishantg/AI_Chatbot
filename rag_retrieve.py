import psycopg2
import logging
from typing import List, Optional
import os

from embeddings import embed

logger = logging.getLogger("rag-retrieve")

# -------------------------------
# DB Config (ENV recommended)
# -------------------------------
DB_CONN = {
    "dbname": os.getenv("RAG_DB_NAME", "rca_agent"),
    "user": os.getenv("RAG_DB_USER", "postgres"),      # Correct
    "password": os.getenv("RAG_DB_PASSWORD", "postgres"),
    "host": os.getenv("RAG_DB_HOST", "localhost"),
    "port": os.getenv("RAG_DB_PORT", "5432"),
}

# -------------------------------
# RAG Retrieval
# -------------------------------
def retrieve_context(
    query: str,
    top_k: int = 3,
    max_query_length: int = 1000,
) -> str:
    """
    Retrieve similar RCA incidents from pgvector.

    Fully safe:
    - Never raises
    - Never blocks RCA
    """

    if not query:
        return "No historical context available."

    try:
        # 1. Sanitize
        safe_query = f"RCA Incident: {query[:max_query_length]}"

        # 2. Embeddings
        vector: Optional[List[float]] = embed(safe_query)

        if not vector:
            logger.warning("Embedding unavailable, skipping RAG")
            return "No historical context available."

        # 3. Query pgvector
        with psycopg2.connect(**DB_CONN, connect_timeout=3) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT content
                    FROM rca_knowledge
                    ORDER BY embedding <=> %s    -- FIXED
                    LIMIT %s
                    """,
                    (vector, int(top_k)),
                )
                rows = cur.fetchall()

        # 4. Format output
        if not rows:
            return "No similar historical incidents found."

        return "\n\n".join(f"- {row[0]}" for row in rows)

    except Exception as e:
        logger.error("RAG retrieval failed", exc_info=e)
        return "Historical context unavailable."