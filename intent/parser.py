"""
intent/parser.py
NL → IntentJSON: sends the user command + graph context to the LLM.
LLM provider is switchable via LLM_PROVIDER env var.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
load_dotenv()

import structlog

from intent.schema import IntentJSON

logger = structlog.get_logger(__name__)

_PROMPT_DIR = Path(__file__).parent / "prompts"


def _load_prompt(name: str) -> str:
    return (_PROMPT_DIR / name).read_text(encoding="utf-8")


class LLMClient:
    """
    Thin LLM abstraction — wraps Groq, Anthropic, or Ollama.
    Selected by LLM_PROVIDER env var (default: groq).
    """

    def __init__(self) -> None:
        self._provider = os.environ.get("LLM_PROVIDER", "groq").lower()
        self._model = os.environ.get("LLM_MODEL", "llama3-70b-8192")
        self._client: Any = None
        self._init_client()

    def _init_client(self) -> None:
        if self._provider == "groq":
            try:
                from groq import Groq
            except ImportError:
                raise ValueError(
                    "The 'groq' package is not installed. Run 'pip install groq' or 'uv pip install groq'."
                )
            api_key = os.environ.get("GROQ_API_KEY")
            if not api_key:
                raise ValueError(
                    "GROQ_API_KEY environment variable is not set. "
                    "Please add your GROQ_API_KEY to the .env file."
                )
            self._client = Groq(api_key=api_key)

        elif self._provider == "anthropic":
            try:
                import anthropic
            except ImportError:
                raise ValueError(
                    "The 'anthropic' package is not installed. Run 'pip install anthropic' or 'uv pip install anthropic'."
                )
            api_key = os.environ.get("ANTHROPIC_API_KEY")
            if not api_key:
                raise ValueError(
                    "ANTHROPIC_API_KEY environment variable is not set. "
                    "Please add your ANTHROPIC_API_KEY to the .env file."
                )
            self._client = anthropic.Anthropic(api_key=api_key)

        elif self._provider == "ollama":
            try:
                import ollama
            except ImportError:
                raise ValueError(
                    "The 'ollama' package is not installed. Run 'pip install ollama' or 'uv pip install ollama', "
                    "or set LLM_PROVIDER=groq in your .env file."
                )
            self._client = ollama

        elif self._provider == "databricks":
            try:
                from openai import OpenAI
            except ImportError:
                raise ValueError(
                    "The 'openai' package is not installed. Run 'pip install openai' or 'uv pip install openai'."
                )
            self._client = OpenAI(
                api_key=os.environ.get("DATABRICKS_TOKEN"),
                base_url=f"{os.environ.get('DATABRICKS_HOST')}/serving-endpoints",
            )
        else:
            raise ValueError(f"Unknown LLM_PROVIDER: {self._provider!r}")

        logger.info("llm.initialized", provider=self._provider, model=self._model)

    def complete(self, system_prompt: str, user_message: str, max_tokens: int = 4096) -> str:
        """Send a chat completion request and return the response text."""
        try:
            if self._provider == "groq":
                resp = self._client.chat.completions.create(
                    model=self._model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_message},
                    ],
                    max_tokens=max_tokens,
                    temperature=0.0,
                )
                return resp.choices[0].message.content

            elif self._provider == "anthropic":
                resp = self._client.messages.create(
                    model=self._model,
                    system=system_prompt,
                    messages=[{"role": "user", "content": user_message}],
                    max_tokens=max_tokens,
                )
                return resp.content[0].text

            elif self._provider == "ollama":
                resp = self._client.chat(
                    model=self._model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_message},
                    ],
                )
                return resp["message"]["content"]

            elif self._provider == "databricks":
                resp = self._client.chat.completions.create(
                    model=self._model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_message},
                    ],
                    max_tokens=max_tokens,
                )
                return resp.choices[0].message.content

            else:
                raise ValueError(f"Unknown provider: {self._provider}")

        except Exception as exc:
            logger.error("llm.completion_failed", provider=self._provider, error=str(exc))
            raise

    def chat(self, prompt: str) -> str:
        """Simple one-shot chat (no system prompt)."""
        return self.complete("You are a helpful assistant.", prompt)


class IntentParser:
    """
    Parses a natural-language migration command into a structured IntentJSON.

    The schema graph context is injected into the system prompt so the LLM
    can resolve table/column names against real objects.
    """

    def __init__(self, llm: LLMClient | None = None) -> None:
        self._llm = llm or LLMClient()
        self._system_prompt_template = _load_prompt("parse_intent.txt")

    def parse(self, nl_command: str, schema_graph_dict: dict[str, Any]) -> IntentJSON:
        """
        Parse a natural-language command and return a raw IntentJSON.

        Args:
            nl_command: User's natural language instruction.
            schema_graph_dict: Serialised SchemaGraph (nodes + edges).
        Returns:
            IntentJSON (entity names unresolved).
        """
        # Trim graph context to avoid massive token counts
        graph_ctx = self._trim_graph_context(schema_graph_dict)
        system_prompt = self._system_prompt_template.replace(
            "{{ schema_graph }}", json.dumps(graph_ctx, indent=2)
        )

        logger.info("intent_parser.parsing", command=nl_command[:100])
        raw_response = self._llm.complete(system_prompt, nl_command)
        logger.debug("intent_parser.raw_response", response=raw_response[:300])

        intent = self._parse_response(raw_response, nl_command)
        return intent

    def _parse_response(self, raw: str, original_command: str) -> IntentJSON:
        """Extract JSON from LLM response and validate with Pydantic."""
        # Strip markdown code fences if present
        text = raw.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(
                l for l in lines
                if not l.strip().startswith("```")
            ).strip()

        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            logger.error("intent_parser.json_parse_failed", raw=raw[:200], error=str(exc))
            raise ValueError(f"LLM returned invalid JSON: {exc}") from exc

        data["user_nl_command"] = original_command
        data["llm_raw_response"] = raw
        return IntentJSON(**data)

    @staticmethod
    def _trim_graph_context(graph: dict[str, Any], max_nodes: int = 30) -> dict[str, Any]:
        """
        Reduce graph JSON size for the prompt — keep first N nodes
        with only name + columns (no stats, no metadata).
        """
        nodes = list(graph.get("nodes", {}).values())[:max_nodes]
        slim_nodes = [
            {
                "id": n.get("id"),
                "name": n.get("name"),
                "qualified_name": n.get("qualified_name"),
                "kind": n.get("kind"),
                "columns": [
                    {"name": c.get("name"), "dtype": c.get("dtype"), "primary_key": c.get("primary_key")}
                    for c in n.get("columns", [])[:30]
                ],
            }
            for n in nodes
        ]
        edges = [
            {
                "source": e.get("source_node_id"),
                "target": e.get("target_node_id"),
                "join_keys": e.get("join_keys", []),
                "confidence": e.get("confidence"),
            }
            for e in list(graph.get("edges", {}).values())[:50]
        ]
        return {"nodes": slim_nodes, "edges": edges}
