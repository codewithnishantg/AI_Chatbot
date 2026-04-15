import json
import logging
import os
from typing import Dict, Any

from langchain_ollama import ChatOllama
from dotenv import load_dotenv

# -------------------------------
# Env & Logging
# -------------------------------
load_dotenv()

logger = logging.getLogger("rca-llm")
logging.basicConfig(level=logging.INFO)

LLM_MODEL = os.getenv("MAIN_LLM_MODEL", "mistral:instruct")
LLM_TIMEOUT = int(os.getenv("LLM_TIMEOUT", "60"))

# -------------------------------
# LLM Initialization
# -------------------------------
llm = ChatOllama(
    model=LLM_MODEL,
    temperature=0,
    timeout=LLM_TIMEOUT,
    keep_alive=-1,
    num_predict=256,
    num_ctx=4096,
    repeat_penalty=1.1,
)

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
- Use ONLY the facts and reference provided
- Do NOT guess or hallucinate
- If data is insufficient, say "Insufficient data"
- Do NOT include explanations outside JSON

Return VALID JSON ONLY.
The output MUST be parseable by json.loads().

{{
  "root_cause": "",
  "evidence": "",
  "blast_radius": "",
  "confidence": "",
  "recommended_actions": ""
}}
"""

# -------------------------------
# RCA Runner (Production Safe)
# -------------------------------
def run_rca(
    facts_text: str,
    context: str,
) -> Dict[str, Any]:
    """
    Runs LLM-based RCA reasoning.
    Never raises.
    Always returns structured output.
    """

    prompt = build_prompt(facts_text, context)

    messages = [
        ("system", "You are a strict RCA engine. Always return valid JSON."),
        ("human", prompt),
    ]

    try:
        response = llm.invoke(messages)
        output = response.content.strip()

        try:
            return json.loads(output)
        except Exception:
            # -------------------------------
            # Single corrective retry
            # -------------------------------
            logger.warning("Invalid JSON from LLM, retrying once")

            retry_messages = messages + [
                (
                    "system",
                    "Your previous response was invalid. "
                    "Return ONLY valid JSON. No text before or after JSON."
                )
            ]

            retry_response = llm.invoke(retry_messages)
            retry_output = retry_response.content.strip()

            try:
                return json.loads(retry_output)
            except Exception:
                logger.error("LLM failed after retry")
                return {
                    "root_cause": "LLM returned invalid JSON",
                    "evidence": retry_output,
                    "blast_radius": "Unknown",
                    "confidence": "Low",
                    "recommended_actions": "Inspect raw LLM output and prompt"
                }

    except Exception as e:
        logger.exception("LLM invocation failed")
        return {
            "root_cause": "LLM invocation failure",
            "evidence": str(e),
            "blast_radius": "Unknown",
            "confidence": "Low",
            "recommended_actions": "Check LLM availability and configuration"
        }

# -------------------------------
# Local Test
# -------------------------------
if __name__ == "__main__":
    facts = """
    API: /upi/pay
    Error: Timeout
    Failed Transactions: 1204
    Dependency: Bank API not responding
    """

    context = """
    Similar incident occurred last week due to downstream bank outage.
    Mitigation involved retry backoff and circuit breaker activation.
    """

    result = run_rca(facts, context)
    print(json.dumps(result, indent=2))