from fastapi import FastAPI, Body, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, Optional
import json
import logging
import uuid
import os
import re
import requests

from rag_retrieve import retrieve_context
from rag_store import store_rca_memory        # ✅ MEMORY STORE
from facts_builder import build_facts
from db_client import get_top_failure

# -------------------------------
# App Init
# -------------------------------
app = FastAPI(title="RCA Agent")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
logger = logging.getLogger("rca-agent")

# -------------------------------
# Config (ENV-driven)
# -------------------------------
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
LLM_MODEL = os.getenv("LLM_MODEL", "mistral7b:latest")
LLM_TIMEOUT = int(os.getenv("LLM_TIMEOUT", "120"))

# -------------------------------
# Request / Response Schemas
# -------------------------------
class RCARequest(BaseModel):
    alert_type: str


class RCAResponse(BaseModel):
    status: str
    request_id: str
    facts: Dict[str, Any]
    rca: Dict[str, Any]


class DBHealthResponse(BaseModel):
    status: str
    db_connected: bool
    sample_data: Optional[Dict[str, Any]] = None
    message: Optional[str] = None


# -------------------------------
# LLM Client
# -------------------------------
def generate_text(prompt: str, request_id: str) -> str:
    """
    Calls Ollama chat API safely.
    - Uses /api/chat (required for chat models)
    - Bypasses system proxies (critical on Windows)
    """
    try:
        response = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": LLM_MODEL,
                "messages": [
                    {"role": "user", "content": prompt}
                ],
                "stream": False
            },
            timeout=LLM_TIMEOUT,
            proxies={
                "http": None,
                "https": None
            }
        )

        response.raise_for_status()
        data = response.json()

        content = data.get("message", {}).get("content")
        if not content:
            raise ValueError(f"Invalid Ollama response: {data}")

        return content.strip()

    except Exception as e:
        logger.exception(f"[{request_id}] LLM invocation failed: {e}")
        raise RuntimeError("LLM invocation failed") from e


# -------------------------------
# Prompt Builder
# -------------------------------
def build_prompt(facts_text: str, context: str) -> str:
    return f"""
You are an expert RCA agent for UPI payment systems.

FACTS:
{facts_text}

REFERENCE:
{context}

Rules:
- Use ONLY the provided facts and reference
- Do NOT hallucinate or guess
- If data is insufficient, explicitly say so
- Return VALID JSON ONLY
- Do NOT add text outside JSON

{{
  "root_cause": "",
  "why": "",
  "evidence": "",
  "timeline": "",
  "impact": "",
  "confidence": "",
  "recommended_actions": ""
}}
"""


# -------------------------------
# RCA API
# -------------------------------
@app.post(
    "/rca",
    response_model=RCAResponse,
    summary="Generate Root Cause Analysis"
)
def generate_rca(
    req: RCARequest = Body(
        ...,
        example={"alert_type": "UPI_TX_FAILURE_SPIKE"}
    )
):
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] RCA request received")

    try:
        # 1️⃣ Build Facts
        facts = build_facts(alert_type=req.alert_type)
        if not facts or "error" in facts:
            return RCAResponse(
                status="error",
                request_id=request_id,
                facts=facts or {},
                rca={
                    "root_cause": "Facts collection failed",
                    "confidence": "Low",
                    "recommended_actions": "Verify DB and ADX availability"
                }
            )

        facts_text = "\n".join(f"{k}: {v}" for k, v in facts.items())
        logger.info(f"[{request_id}] Facts collected")

        # 2️⃣ RAG Retrieval
        try:
            context = retrieve_context(facts_text)
        except Exception:
            context = "No historical context available"

        # 3️⃣ Prompt Build
        prompt = build_prompt(facts_text, context)

        # 4️⃣ LLM Response
        raw_output = generate_text(prompt, request_id)

        # 5️⃣ Extract JSON from LLM response
        match = re.search(r"\{.*\}", raw_output, re.DOTALL)
        if not match:
            raise ValueError("LLM did not return valid JSON")

        parsed_output = json.loads(match.group())

        # 6️⃣ Store RCA Memory (self-learning)
        try:
            store_rca_memory(
                alert_type=req.alert_type,
                facts=facts,
                rca=parsed_output,
            )
        except Exception as e:
            logger.error(f"[{request_id}] Failed to store RCA memory: {e}")

        # 7️⃣ Final Output
        return RCAResponse(
            status="success",
            request_id=request_id,
            facts=facts,
            rca=parsed_output
        )

    except Exception as e:
        logger.exception(f"[{request_id}] RCA execution failed")
        raise HTTPException(
            status_code=500,
            detail={
                "request_id": request_id,
                "error": str(e)
            }
        )


# -------------------------------
# DB Health Check
# -------------------------------
@app.get(
    "/health/db",
    response_model=DBHealthResponse,
    summary="Postgres DB health check"
)
def db_health():
    try:
        bank, error_code, failure_count = get_top_failure()

        return DBHealthResponse(
            status="ok",
            db_connected=True,
            sample_data={
                "bank": bank,
                "error_code": error_code,
                "failure_count": failure_count
            }
        )

    except Exception as e:
        return DBHealthResponse(
            status="error",
            db_connected=False,
            message=str(e)
        )


# -------------------------------
# Root Endpoint
# -------------------------------
@app.get("/", summary="Service Health")
def home():
    return {"message": "RCA Agent is running 🚀"}