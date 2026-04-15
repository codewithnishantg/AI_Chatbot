import logging
import os
from typing import List, Optional

import requests

# -------------------------------
# Logging
# -------------------------------
logger = logging.getLogger("embedding-client")
logging.basicConfig(level=logging.INFO)

# -------------------------------
# Config (ENV driven)
# -------------------------------
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
EMBED_MODEL = os.getenv("EMBED_MODEL", "nomic-embed-text")
EMBED_TIMEOUT = int(os.getenv("EMBED_TIMEOUT", "30"))
MAX_TEXT_LENGTH = int(os.getenv("EMBED_MAX_TEXT_LENGTH", "2000"))

# -------------------------------
# Embedding Client
# -------------------------------
def embed(text: str) -> Optional[List[float]]:
    """
    Generate vector embeddings using Ollama.

    - Never raises
    - Returns List[float] or None
    - Safe for RAG
    """

    if not text:
        logger.warning("Empty text passed to embed()")
        return None

    try:
        safe_text = text[:MAX_TEXT_LENGTH]

        response = requests.post(
            f"{OLLAMA_URL}/api/embed",
            json={
                "model": EMBED_MODEL,
                "prompt": safe_text
            },
            timeout=EMBED_TIMEOUT,
            proxies={                 # ✅ CRITICAL FIX
                "http": None,
                "https": None
            }
        )

        response.raise_for_status()
        data = response.json()

        embedding = data.get("embedding")

        # Some Ollama versions return nested lists
        if isinstance(embedding, list) and embedding and isinstance(embedding[0], list):
            embedding = embedding[0]

        if not isinstance(embedding, list):
            raise ValueError(f"Invalid embedding format: {type(embedding)}")

        return embedding

    except Exception as e:
        logger.error("Embedding generation failed", exc_info=e)
        return None