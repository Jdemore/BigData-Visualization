"""OpenAI client — singleton, structured JSON output."""

import json
import os

import openai

MODEL = "gpt-4o-mini"

_client: openai.OpenAI | None = None


def get_client() -> openai.OpenAI:
    """Reuse a single client instance. Reads OPENAI_API_KEY from env."""
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
    """Call GPT with structured JSON output. Returns parsed dict."""
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
    """Send a throwaway request at startup to confirm the API key works."""
    try:
        query_llm("Respond with: {}", system="Return valid JSON only.")
        print(f"  Using model: {MODEL}")
    except Exception as e:
        print(f"  Warning: model warm-up failed: {e}")
