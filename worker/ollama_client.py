import httpx

OLLAMA_BASE = "http://localhost:11434"


def generate(model: str, prompt: str, timeout: int = 300) -> str:
    """Send a prompt to the local Ollama instance and return the response text."""
    with httpx.Client(timeout=timeout) as client:
        resp = client.post(
            f"{OLLAMA_BASE}/api/generate",
            json={"model": model, "prompt": prompt, "stream": False},
        )
        resp.raise_for_status()
        return resp.json()["response"]


def list_models() -> list[str]:
    """Return the names of models currently available in Ollama."""
    with httpx.Client(timeout=10) as client:
        resp = client.get(f"{OLLAMA_BASE}/api/tags")
        resp.raise_for_status()
        return [m["name"] for m in resp.json().get("models", [])]
