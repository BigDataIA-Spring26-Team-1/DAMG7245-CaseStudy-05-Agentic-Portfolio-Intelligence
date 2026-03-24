from __future__ import annotations
 
import os
import time
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, List, Optional
 
try:
    from litellm import completion
except Exception:  # pragma: no cover
    completion = None
 
 
class TaskType(str, Enum):
    JUSTIFICATION = "justification"
    HYDE = "hyde"
    IC_PREP = "ic_prep"
    ANALYST_NOTES = "analyst_notes"
    GENERAL = "general"
 
 
@dataclass(frozen=True)
class ModelCandidate:
    provider: str
    model: str
    api_key_env: Optional[str] = None
 
 
@dataclass(frozen=True)
class LLMResponse:
    text: str
    provider: str
    model: str
    task_type: str
    fallback_used: bool
    latency_ms: int
    raw: Optional[Dict[str, Any]] = None
 
 
class LiteLLMRouter:
    """
    Multi-provider LLM router with fallback support.
 
    Current configuration:
    - Primary provider: OpenAI
    - Fallback provider: Gemini
 
    Goals:
    - route by task type
    - try providers in order
    - fail clearly if no provider is configured
    - keep response metadata for debugging/auditing
    """
 
    def __init__(self) -> None:
        self.timeout = int(os.getenv("LLM_REQUEST_TIMEOUT", "60"))
        self.max_retries = int(os.getenv("LLM_MAX_RETRIES", "2"))
 
        self.routes: Dict[TaskType, List[ModelCandidate]] = {
            TaskType.JUSTIFICATION: [
                ModelCandidate(
                    "openai",
                    os.getenv(
                        "LLM_JUSTIFICATION_MODEL",
                        os.getenv("OPENAI_MODEL", "openai/gpt-4o-mini"),
                    ),
                    "OPENAI_API_KEY",
                ),
                ModelCandidate(
                    "gemini",
                    os.getenv(
                        "LLM_JUSTIFICATION_FALLBACK_MODEL",
                        os.getenv("GEMINI_MODEL", "gemini/gemini-2.5-flash"),
                    ),
                    "GEMINI_API_KEY",
                ),
            ],
            TaskType.HYDE: [
                ModelCandidate(
                    "openai",
                    os.getenv(
                        "LLM_HYDE_MODEL",
                        os.getenv("OPENAI_MODEL", "openai/gpt-4o-mini"),
                    ),
                    "OPENAI_API_KEY",
                ),
                ModelCandidate(
                    "gemini",
                    os.getenv(
                        "LLM_HYDE_FALLBACK_MODEL",
                        os.getenv("GEMINI_MODEL", "gemini/gemini-2.5-flash"),
                    ),
                    "GEMINI_API_KEY",
                ),
            ],
            TaskType.IC_PREP: [
                ModelCandidate(
                    "openai",
                    os.getenv(
                        "LLM_IC_PREP_MODEL",
                        os.getenv("OPENAI_MODEL", "openai/gpt-4o-mini"),
                    ),
                    "OPENAI_API_KEY",
                ),
                ModelCandidate(
                    "gemini",
                    os.getenv(
                        "LLM_IC_PREP_FALLBACK_MODEL",
                        os.getenv("GEMINI_MODEL", "gemini/gemini-2.5-flash"),
                    ),
                    "GEMINI_API_KEY",
                ),
            ],
            TaskType.ANALYST_NOTES: [
                ModelCandidate(
                    "openai",
                    os.getenv(
                        "LLM_ANALYST_MODEL",
                        os.getenv("OPENAI_MODEL", "openai/gpt-4o-mini"),
                    ),
                    "OPENAI_API_KEY",
                ),
                ModelCandidate(
                    "gemini",
                    os.getenv(
                        "LLM_ANALYST_FALLBACK_MODEL",
                        os.getenv("GEMINI_MODEL", "gemini/gemini-2.5-flash"),
                    ),
                    "GEMINI_API_KEY",
                ),
            ],
            TaskType.GENERAL: [
                ModelCandidate(
                    "openai",
                    os.getenv("OPENAI_MODEL", "openai/gpt-4o-mini"),
                    "OPENAI_API_KEY",
                ),
                ModelCandidate(
                    "gemini",
                    os.getenv("GEMINI_MODEL", "gemini/gemini-2.5-flash"),
                    "GEMINI_API_KEY",
                ),
            ],
        }
 
    def complete(
        self,
        *,
        task_type: TaskType,
        user_prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0.2,
        max_tokens: int = 800,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> LLMResponse:
        if not user_prompt or not user_prompt.strip():
            raise ValueError("user_prompt is required")
 
        if completion is None:
            raise RuntimeError(
                "LiteLLM is not installed. Add 'litellm' to dependencies before using the LLM router."
            )
 
        candidates = self.routes.get(task_type) or self.routes[TaskType.GENERAL]
        errors: List[str] = []
 
        for idx, candidate in enumerate(candidates):
            api_key = os.getenv(candidate.api_key_env) if candidate.api_key_env else None
            if candidate.api_key_env and not api_key:
                errors.append(f"{candidate.provider}: missing {candidate.api_key_env}")
                continue
 
            try:
                start = time.perf_counter()
 
                messages: List[Dict[str, str]] = []
                if system_prompt:
                    messages.append({"role": "system", "content": system_prompt})
                messages.append({"role": "user", "content": user_prompt})
 
                kwargs: Dict[str, Any] = {
                    "model": candidate.model,
                    "messages": messages,
                    "temperature": temperature,
                    "max_tokens": max_tokens,
                    "timeout": self.timeout,
                }
 
                if api_key:
                    kwargs["api_key"] = api_key
 
                response = completion(**kwargs)
                latency_ms = int((time.perf_counter() - start) * 1000)
 
                text = self._extract_text(response)
                if not text.strip():
                    raise RuntimeError(f"{candidate.provider} returned empty content")
 
                raw_payload = {
                    "provider": candidate.provider,
                    "model": candidate.model,
                    "metadata": metadata or {},
                }
 
                return LLMResponse(
                    text=text,
                    provider=candidate.provider,
                    model=candidate.model,
                    task_type=task_type.value,
                    fallback_used=idx > 0,
                    latency_ms=latency_ms,
                    raw=raw_payload,
                )
 
            except Exception as exc:
                errors.append(f"{candidate.provider}: {str(exc)}")
                continue
 
        raise RuntimeError(
            "All LLM providers failed for task_type="
            f"{task_type.value}. Errors: {' | '.join(errors)}"
        )
 
    def _extract_text(self, response: Any) -> str:
        """
        Normalize LiteLLM response across providers.
        """
        try:
            choice = response.choices[0]
            message = getattr(choice, "message", None)
            if message is not None:
                content = getattr(message, "content", None)
                if isinstance(content, str):
                    return content
                if isinstance(content, list):
                    parts: List[str] = []
                    for item in content:
                        if isinstance(item, dict) and item.get("type") == "text":
                            parts.append(str(item.get("text", "")))
                        elif hasattr(item, "text"):
                            parts.append(str(item.text))
                    return "\n".join(p for p in parts if p)
 
            text = getattr(choice, "text", None)
            if isinstance(text, str):
                return text
        except Exception:
            pass
 
        try:
            if isinstance(response, dict):
                choices = response.get("choices", [])
                if choices:
                    msg = choices[0].get("message", {})
                    content = msg.get("content")
                    if isinstance(content, str):
                        return content
                    if isinstance(content, list):
                        return "\n".join(
                            str(item.get("text", ""))
                            for item in content
                            if isinstance(item, dict)
                        )
                    text = choices[0].get("text")
                    if isinstance(text, str):
                        return text
        except Exception:
            pass
 
        return ""
 
    def healthcheck(self) -> Dict[str, Any]:
        """
        Reports router readiness without making a provider call.
        """
        out: Dict[str, Any] = {
            "litellm_installed": completion is not None,
            "providers": {},
        }
 
        provider_envs = {
            "openai": "OPENAI_API_KEY",
            "gemini": "GEMINI_API_KEY",
        }
 
        for provider, env_key in provider_envs.items():
            out["providers"][provider] = {
                "configured": bool(os.getenv(env_key)),
                "env_key": env_key,
            }
 
        out["routes"] = {
            task.value: [asdict(candidate) for candidate in candidates]
            for task, candidates in self.routes.items()
        }
        return out