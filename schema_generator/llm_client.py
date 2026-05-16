"""Groq LLM client and project-root / env helpers."""

import os
import time
import hashlib
import json
from pathlib import Path
from typing import Dict

import requests


# LLM Response Cache
_LLM_CACHE_DIR = None
_LLM_CACHE_ENABLED = False  # DISABLED - caching can cause issues with iterative development


def _get_cache_dir() -> Path:
    """Get or create the LLM cache directory."""
    global _LLM_CACHE_DIR
    if _LLM_CACHE_DIR is None:
        cache_dir = Path(os.environ.get("LLM_CACHE_DIR", ".llm_cache"))
        cache_dir.mkdir(exist_ok=True)
        _LLM_CACHE_DIR = cache_dir
    return _LLM_CACHE_DIR


def _get_cache_key(prompt: str, system: str) -> str:
    """Generate cache key from prompt and system message."""
    combined = f"{system}|||{prompt}"
    return hashlib.md5(combined.encode()).hexdigest()


def _get_cached_response(prompt: str, system: str) -> str:
    """Get cached LLM response if available."""
    if not _LLM_CACHE_ENABLED or os.environ.get("DISABLE_LLM_CACHE") == "1":
        return None
    
    cache_key = _get_cache_key(prompt, system)
    cache_file = _get_cache_dir() / f"{cache_key}.json"
    
    if cache_file.exists():
        try:
            with open(cache_file) as f:
                data = json.load(f)
                return data.get("response")
        except Exception:
            return None
    
    return None


def _save_cached_response(prompt: str, system: str, response: str):
    """Save LLM response to cache."""
    if not _LLM_CACHE_ENABLED or os.environ.get("DISABLE_LLM_CACHE") == "1":
        return
    
    cache_key = _get_cache_key(prompt, system)
    cache_file = _get_cache_dir() / f"{cache_key}.json"
    
    try:
        with open(cache_file, 'w') as f:
            json.dump({
                "prompt_hash": cache_key,
                "response": response,
                "timestamp": time.time()
            }, f)
    except Exception as e:
        # Don't fail if caching fails
        print(f"  ⚠ Warning: Failed to cache LLM response: {e}")


def _load_env(project_root: Path) -> Dict[str, str]:
    env: Dict[str, str] = {}
    env_path = project_root / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip().strip('"')
    env.update({k: v for k, v in os.environ.items() if k in env or k.startswith(("GROQ", "OPENROUTER"))})
    
    groq_keys = []
    for i in range(1, 10):
        key = env.get(f"GROQ_API_KEY_{i}")
        if key:
            groq_keys.append(key)
    
    # If no numbered keys, use single key
    if not groq_keys and env.get("GROQ_API_KEY"):
        groq_keys = [env.get("GROQ_API_KEY")]
    
    env["_GROQ_API_KEYS"] = groq_keys
    
  
    openrouter_keys = []
    for i in range(1, 10):
        key = env.get(f"OPENROUTER_API_KEY_{i}")
        if key:
            openrouter_keys.append(key)
    
    if not openrouter_keys and env.get("OPENROUTER_API_KEY"):
        openrouter_keys = [env.get("OPENROUTER_API_KEY")]
    
    env["_OPENROUTER_API_KEYS"] = openrouter_keys
    return env


def find_project_root(start: Path) -> Path:
    for parent in [start.resolve()] + list(start.resolve().parents):
        if (parent / ".env").exists() or (parent / "pyproject.toml").exists():
            return parent
    return Path(__file__).resolve().parent.parent


def make_llm_client(project_root: Path = None, model_override: str = None):
    """Return a callable llm(prompt, system='') -> str using Groq or OpenRouter.
    
    Supports multiple API keys for rotation to avoid rate limits.
    Add GROQ_API_KEY_1, GROQ_API_KEY_2, etc. or OPENROUTER_API_KEY to .env file.
    
    Args:
        project_root: Project root path (auto-detected if None)
        model_override: Override the model from .env (useful for fixer models)
    """
    env = _load_env(find_project_root(Path(__file__)))
    groq_keys = env.get("_GROQ_API_KEYS", [])
    openrouter_keys = env.get("_OPENROUTER_API_KEYS", [])
    
    # Determine provider
    if openrouter_keys:
        provider = "openrouter"
        api_keys = openrouter_keys
        base_url = "https://openrouter.ai/api/v1/chat/completions"
        default_model = model_override or env.get("OPENROUTER_MODEL", "openai/gpt-4o-mini")
        print(f"  ℹ Using OpenRouter with {len(api_keys)} API key(s)")
    elif groq_keys:
        provider = "groq"
        api_keys = groq_keys
        base_url = "https://api.groq.com/openai/v1/chat/completions"
        default_model = model_override or env.get("GROQ_MODEL", "llama-3.3-70b-versatile")
        print(f"  ℹ Using Groq with {len(api_keys)} API key(s)")
    else:
        raise ValueError("No API keys found. Set GROQ_API_KEY or OPENROUTER_API_KEY in .env or as environment variables.")
    
    model = model_override or default_model
    current_key_index = 0
    
    if len(api_keys) > 1:
        print(f"  ℹ Using {len(api_keys)} API keys for rotation")

    def llm(prompt: str, system: str = "You are an expert Python developer specialising in BioCypher knowledge graph adapters.") -> str:
        nonlocal current_key_index
        
        # Check cache first
        cached_response = _get_cached_response(prompt, system)
        if cached_response:
            print(f"  ✓ Using cached LLM response")
            return cached_response
        
     
        prompt_chars = len(prompt) + len(system)
        if prompt_chars > 120000:
            print(f"  ⚠ Warning: Prompt is very large (~{prompt_chars:,} chars)")
            print(f"  → This may cause 413 Payload Too Large errors")
        
        backoff = 30 
        
        for attempt in range(6):  
            api_key = api_keys[current_key_index % len(api_keys)]
            
            try:
                resp = requests.post(
                    base_url,
                    headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                    json={
                        "model": model,
                        "messages": [
                            {"role": "system", "content": system},
                            {"role": "user",   "content": prompt},
                        ],
                        "temperature": 0.2,
                        "max_tokens": 8192,  # prevent truncation
                    },
                    timeout=120,
                )
                
                if resp.status_code == 413:
                    # Payload too large - cannot retry with same prompt
                    raise Exception(f"Prompt too large ({prompt_chars:,} chars, ~{prompt_chars//4:,} tokens). {provider.title()} returned 413. The schema_config or example adapter is too large - check prompt_builder.py truncation.")
                
                if resp.status_code == 429:
                    if len(api_keys) > 1:
                        current_key_index += 1
                        next_key_num = (current_key_index % len(api_keys)) + 1
                        print(f"  ⚠ Rate limited (429). Switching to API key #{next_key_num}...")
                        time.sleep(5) 
                    else:
                        print(f"  ⚠ Rate limited (429). Waiting {backoff}s before retry {attempt + 1}/6...")
                        time.sleep(backoff)
                        backoff = min(backoff * 2, 180) 
                    continue
                
                resp.raise_for_status()
                response_text = resp.json()["choices"][0]["message"]["content"]
                
                # Cache the response
                _save_cached_response(prompt, system, response_text)
                
                return response_text
                
            except requests.exceptions.RequestException as e:
                if "413" in str(e):
                    raise Exception(f"Prompt too large ({prompt_chars:,} chars). {provider.title()} returned 413. Reduce prompt size in prompt_builder.py")
                raise
                
        raise Exception("Max retries exceeded for LLM call (Rate Limiting)")

    return llm
