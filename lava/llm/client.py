"""Thin wrapper over the OpenAI chat completions API. Centralizes the model id,
temperature, and JSON-mode response format so callers never set them ad-hoc."""

import json
import os

import openai

MODEL = "gpt-4o-mini"

_client: openai.OpenAI | None = None


def get_client() -> openai.OpenAI:
    """Lazy singleton. Keeping one HTTP client across requests reuses the
    underlying connection pool instead of reopening TLS on every LLM call."""
    global _client
    if _client is None:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError(
                "OPENAI_API_KEY not set. Get a key at https://platform.openai.com/api-keys"
            )
        _client = openai.OpenAI(api_key=api_key)
    return _client


def query_llm(prompt: str, system: str) -> dict:
    """One-shot request. temperature=0.1 keeps the output reproducible enough
    that identical prompts nearly always yield identical VizSpecs."""
    client = get_client()
    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_object"},
        temperature=0.1,
        max_tokens=1024,
    )
    text = response.choices[0].message.content
    if not text:
        raise ValueError("GPT returned empty response")
    return json.loads(text)


def warm_model() -> None:
    """Validate API key and prime the connection at startup so the first real
    user query doesn't pay the cold-start TLS handshake."""
    try:
        query_llm("Respond with: {}", system="Return valid JSON only.")
        print(f"  Using model: {MODEL}")
    except Exception as e:
        print(f"  Warning: model warm-up failed: {e}")
