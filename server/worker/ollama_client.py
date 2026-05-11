import httpx

OLLAMA_BASE = "http://localhost:11434"


def generate(model: str, prompt: str, timeout: int = 300) -> str:
    """Send a prompt to the local Ollama instance and return the response text."""
    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.post(
                f"{OLLAMA_BASE}/api/generate",
                json={"model": model, "prompt": prompt, "stream": False},
            )
            resp.raise_for_status()
            return resp.json()["response"]
    except Exception:
        # Local dev fallback so the system still functions without Ollama installed/running.
        return (
            "[Mock worker] Ollama is not available on this machine.\n\n"
            f"You asked: {prompt}\n\n"
            "Start Ollama on http://localhost:11434 (or set OLLAMA_URL) to get real model responses."
        )


def list_models() -> list[str]:
    """Return the names of models currently available in Ollama."""
    with httpx.Client(timeout=10) as client:
        resp = client.get(f"{OLLAMA_BASE}/api/tags")
        resp.raise_for_status()
        return [m["name"] for m in resp.json().get("models", [])]
