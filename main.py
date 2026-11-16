#!/usr/bin/env python3
"""free-model-aggregator-01: high-performance robust multi-provider aggregator.

Authored and optimized for free-model-aggregator-01: one server, ten free
providers, two client wires. Reads the trial brief ($MAKEFUN_PROMPT_FILE),
builds the catalog, registers with the env, serves OpenAI Chat Completions
and Anthropic Messages (JSON + SSE) at /v1/*, routing requests to the highest-rated
free model meeting requirements, with resilient failover on 402/429.

Deterministic by construction: every choice derives from the brief, the request
and the providers' answers. No wall clock, no randomness, strictly serialized
per provider.
"""

from __future__ import annotations

import json
import os
import re
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PROTOCOL_VERSION = 1
POLL_MS = 100
CALL_TIMEOUT_S = 20
SSE_DELTA_CHARS = 64
FIXED_CREATED = 1767225600
MAX_BODY_BYTES = 32 * 1024 * 1024

ENV_URL = (os.environ.get("MAKEFUN_ENV_URL") or "").rstrip("/")
PROMPT_FILE = os.environ.get("MAKEFUN_PROMPT_FILE") or ""
TRIAL_NOW = os.environ.get("MAKEFUN_TRIAL_NOW") or ""

PROVIDERS_SCHEMA = "free-model-aggregator-01/providers/v1"
RATINGS_SCHEMA = "free-model-aggregator-01/ratings/v1"
WORKLOAD_SCHEMA = "free-model-aggregator-01/workload/v1"


def log(message: str) -> None:
    sys.stderr.write(f"aggregator-v1: {message}\n")
    sys.stderr.flush()


# ---------------------------------------------------------------- the brief


def extract_json_with_schema(text: str, schema: str) -> dict:
    """Pull one compact JSON object out of the brief by its schema marker."""
    marker = f'"schema":"{schema}"'
    at = text.find(marker)
    if at < 0:
        m = re.search(r'"schema"\s*:\s*"' + re.escape(schema) + '"', text)
        if m is None:
            raise ValueError(f"brief carries no {schema} document")
        at = m.start()
    start = text.rfind("{", 0, at)
    if start < 0:
        raise ValueError(f"no object opens the {schema} document")
    depth = 0
    in_string = False
    escape = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return json.loads(text[start : i + 1])
    raise ValueError(f"the {schema} document never closes")


def is_record(v) -> bool:
    return isinstance(v, dict)


def blocks_text(content) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts = []
    for block in content:
        if is_record(block) and block.get("type") == "text" and isinstance(block.get("text"), str):
            parts.append(block["text"])
    return "\n".join(parts)


def string_list(v) -> list:
    if isinstance(v, str):
        return [v]
    if not isinstance(v, list):
        return []
    return [x for x in v if isinstance(x, str)]


class Brief:
    """The provider directory, the quality table and the workload manifest."""

    def __init__(self, path: str):
        self.error = None
        self.directory = {"providers": []}
        self.ratings = {"ratings": [], "best": []}
        self.workload = {"manifest": []}
        if not path:
            self.error = "MAKEFUN_PROMPT_FILE is not set"
            return
        try:
            with open(path, "r", encoding="utf-8") as fh:
                text = fh.read()
            self.directory = extract_json_with_schema(text, PROVIDERS_SCHEMA)
            self.ratings = extract_json_with_schema(text, RATINGS_SCHEMA)
            self.workload = extract_json_with_schema(text, WORKLOAD_SCHEMA)
        except Exception as err:  # noqa: BLE001
            self.error = f"cannot read the brief: {err}"

    @property
    def providers(self) -> list:
        return self.directory.get("providers", [])

    @property
    def manifest(self) -> list:
        return self.workload.get("manifest", [])


brief = Brief(PROMPT_FILE)


# ---------------------------------------------------------------- the catalog


def build_catalog() -> dict:
    """Build the catalog document: one entry per provider, its free models with context lengths."""
    rows: dict[str, list] = {}
    for row in brief.ratings.get("ratings", []):
        provider = row.get("provider")
        if provider is None:
            continue
        models = rows.setdefault(provider, [])
        models.append(
            {
                "id": row.get("model"),
                "free": row.get("free") is True,
                "context_length": row.get("context_length"),
            }
        )
    providers = []
    for entry in brief.providers:
        providers.append(
            {
                "id": entry.get("id"),
                "base_url": entry.get("path"),
                "key_env": entry.get("key_env", ""),
                "wire": entry.get("wire"),
                "models": rows.get(entry.get("id"), []),
            }
        )
    return {"providers": providers}


CATALOG = build_catalog()
CATALOG_TEXT = json.dumps(CATALOG)


def free_model_ids() -> list:
    seen: set = set()
    out = []
    for provider in CATALOG["providers"]:
        for model in provider["models"]:
            if model["free"] is True and model["id"] not in seen:
                seen.add(model["id"])
                out.append(model["id"])
    return out


# ---------------------------------------------------------------- the router


class ProviderRefused(Exception):
    def __init__(self, provider: str, model, status: int, code: str):
        super().__init__(f"{provider} refused with {status} {code}")
        self.provider = provider
        self.model = model
        self.status = status
        self.code = code


class ProviderError(Exception):
    def __init__(self, provider: str, status: int, detail: str):
        super().__init__(f"{provider} failed: {detail}")
        self.provider = provider
        self.status = status
        self.detail = detail


class Router:
    """Picks the best free candidate per request and fails over on refusals."""

    COOLDOWN_REQUESTS = 2

    def __init__(self):
        self.order = {entry.get("id"): i for i, entry in enumerate(brief.providers)}
        self.rating: dict = {}
        self.caps: dict = {}
        for row in brief.ratings.get("ratings", []):
            key = (row.get("provider"), row.get("model"))
            self.rating[key] = row.get("rating_milli", 0)
            self.caps[key] = {
                "tools": row.get("tools") is True,
                "vision": row.get("vision") is True,
                "json": row.get("json") is True,
                "context_length": row.get("context_length"),
                "free": row.get("free") is True,
            }
        self.wires = {entry.get("id"): entry.get("wire") for entry in brief.providers}
        self.last_refusal: dict = {}
        self.permanent: set = set()
        self.lock = threading.Lock()
        self.request_index = 0

    def meets(self, provider: str, model: str, requires: dict) -> bool:
        caps = self.caps.get((provider, model))
        if caps is None or not caps["free"]:
            return False
        if requires.get("tools") is True and not caps["tools"]:
            return False
        if requires.get("vision") is True and not caps["vision"]:
            return False
        if requires.get("json") is True and not caps["json"]:
            return False
        min_context = requires.get("min_context")
        if min_context is not None:
            context = caps["context_length"]
            if context is None or context < min_context:
                return False
        return True

    def candidates(self, api: str, requires: dict) -> list:
        out = []
        for entry in brief.providers:
            provider = entry.get("id")
            for row in brief.ratings.get("ratings", []):
                if row.get("provider") != provider:
                    continue
                model = row.get("model")
                if not self.meets(provider, model, requires):
                    continue
                wire_match = 0 if self.wires.get(provider) == api else 1
                out.append((-(row.get("rating_milli") or 0), wire_match, self.order.get(provider, 999), model, provider))
        out.sort()
        return [(provider, model) for _, _, _, model, provider in out]

    def usable(self, provider: str, at: int) -> bool:
        if provider in self.permanent:
            return False
        last = self.last_refusal.get(provider)
        return last is None or at - last >= self.COOLDOWN_REQUESTS

    def note_refusal(self, provider: str, at: int) -> None:
        last = self.last_refusal.get(provider)
        if last is not None and at - last >= self.COOLDOWN_REQUESTS:
            self.permanent.add(provider)
        self.last_refusal[provider] = at

    def note_success(self, provider: str) -> None:
        self.last_refusal.pop(provider, None)

    def next_request_index(self) -> int:
        with self.lock:
            index = self.request_index
            self.request_index += 1
            return index


router = Router()


# ---------------------------------------------------------------- the provider client


def provider_headers(entry: dict) -> dict:
    auth = entry.get("auth")
    key = entry.get("key", "")
    if auth == "bearer":
        return {"Authorization": f"Bearer {key}"}
    if auth == "x-api-key":
        return {"x-api-key": key}
    if auth == "x-goog-api-key":
        return {"x-goog-api-key": key}
    return {}


def http_json(method: str, url: str, headers: dict, body=None, timeout: float = CALL_TIMEOUT_S):
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("Content-Type", "application/json")
    for name, value in headers.items():
        request.add_header(name, value)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = response.read().decode("utf-8")
            return response.status, payload
    except urllib.error.HTTPError as err:
        try:
            payload = err.read().decode("utf-8")
        except Exception:  # noqa: BLE001
            payload = ""
        return err.code, payload
    except Exception as err:  # noqa: BLE001
        raise ProviderError("", 0, f"transport: {err}") from err


def parse_provider_error(wire: str, status: int, payload: str) -> str:
    try:
        value = json.loads(payload)
    except Exception:  # noqa: BLE001
        return ""
    if not is_record(value):
        return ""
    if wire == "anthropic":
        error = value.get("error")
        if is_record(error):
            return str(error.get("type") or "")
        return ""
    error = value.get("error")
    if isinstance(error, str):
        return error
    if is_record(error):
        return str(error.get("code") or error.get("status") or "")
    if wire == "gemini" and is_record(value):
        return str(value.get("status") or "")
    return ""


def call_provider(entry: dict, model: str, upstream_body: dict):
    """Execute one provider inference call, non-streaming. Returns (text, usage, stop)."""
    provider = entry["id"]
    wire = entry.get("wire")
    base = ENV_URL + entry.get("path", "")
    headers = provider_headers(entry)
    if wire == "openai":
        url = f"{base}/v1/chat/completions"
    elif wire == "anthropic":
        url = f"{base}/v1/messages"
        headers["anthropic-version"] = "2023-06-01"
    else:
        quoted = urllib.parse.quote(model, safe="")
        url = f"{base}/v1beta/models/{quoted}:generateContent"
    status, payload = http_json("POST", url, headers, upstream_body)
    if status < 200 or status >= 300:
        code = parse_provider_error(wire, status, payload)
        raise ProviderRefused(provider, model, status, code or "error")
    try:
        value = json.loads(payload)
    except Exception as err:  # noqa: BLE001
        raise ProviderError(provider, status, "the answer is not JSON") from err
    if wire == "openai":
        choices = value.get("choices") or []
        choice = choices[0] if choices and is_record(choices[0]) else {}
        message = choice.get("message") or {}
        text = message.get("content") if isinstance(message.get("content"), str) else blocks_text(message.get("content"))
        usage = value.get("usage") or {}
        return (
            text or "",
            (usage.get("prompt_tokens"), usage.get("completion_tokens")),
            choice.get("finish_reason"),
        )
    if wire == "anthropic":
        text = "".join(
            block.get("text", "")
            for block in (value.get("content") or [])
            if is_record(block) and block.get("type") == "text"
        )
        usage = value.get("usage") or {}
        return (
            text,
            (usage.get("input_tokens"), usage.get("output_tokens")),
            value.get("stop_reason"),
        )
    candidates = value.get("candidates") or []
    candidate = candidates[0] if candidates and is_record(candidates[0]) else {}
    content = candidate.get("content") or {}
    text = "".join(
        part.get("text", "") for part in (content.get("parts") or []) if is_record(part)
    )
    usage = value.get("usageMetadata") or {}
    return (
        text,
        (usage.get("promptTokenCount"), usage.get("candidatesTokenCount")),
        candidate.get("finishReason"),
    )


# ---------------------------------------------------------------- the translations


def data_url_parts(url: str):
    match = re.match(r"^data:([^,]*);base64,(.*)$", url, re.S)
    if match:
        return match.group(1) or "image/png", match.group(2)
    return None, url


def openai_messages_from_anthropic(body: dict) -> list:
    messages = []
    system_text = blocks_text(body.get("system"))
    if system_text:
        messages.append({"role": "system", "content": system_text})
    for message in body.get("messages") or []:
        if not is_record(message):
            continue
        role = message.get("role", "user")
        content = message.get("content")
        if isinstance(content, str):
            messages.append({"role": role, "content": content})
            continue
        texts = []
        images = []
        if isinstance(content, list):
            for block in content:
                if not is_record(block):
                    continue
                if block.get("type") == "text" and isinstance(block.get("text"), str):
                    texts.append(block["text"])
                elif block.get("type") == "image" and is_record(block.get("source")):
                    source = block["source"]
                    if source.get("type") == "base64" and isinstance(source.get("data"), str):
                        mime = source.get("media_type", "image/png")
                        images.append({"type": "image_url", "image_url": {"url": f"data:{mime};base64,{source['data']}"}})
                    elif source.get("type") == "url" and isinstance(source.get("url"), str):
                        images.append({"type": "image_url", "image_url": {"url": source["url"]}})
        parts = []
        if texts:
            parts.append({"type": "text", "text": "\n".join(texts)})
        parts.extend(images)
        messages.append({"role": role, "content": parts if parts else ""})
    return messages


def openai_tools_from_anthropic(tools) -> list:
    out = []
    for tool in tools or []:
        if is_record(tool) and isinstance(tool.get("name"), str):
            out.append(
                {
                    "type": "function",
                    "function": {
                        "name": tool["name"],
                        "description": tool.get("description", ""),
                        "parameters": tool.get("input_schema") or {"type": "object"},
                    },
                }
            )
    return out


def upstream_for_openai_provider(api: str, model: str, body: dict) -> dict:
    if api == "openai":
        out = dict(body)
        out["model"] = model
        out["stream"] = False
        out.pop("stream_options", None)
        return out
    out = {"model": model, "stream": False}
    out["messages"] = openai_messages_from_anthropic(body)
    tools = openai_tools_from_anthropic(body.get("tools"))
    if tools:
        out["tools"] = tools
    if isinstance(body.get("max_tokens"), (int, float)):
        out["max_tokens"] = body["max_tokens"]
    stops = string_list(body.get("stop_sequences"))
    if stops:
        out["stop"] = stops
    if "temperature" in body:
        out["temperature"] = body["temperature"]
    if "top_p" in body:
        out["top_p"] = body["top_p"]
    return out


def gemini_body_from_messages(contents: list, system_text: str, tools, generation: dict) -> dict:
    out: dict = {"contents": contents}
    if system_text:
        out["systemInstruction"] = {"parts": [{"text": system_text}]}
    if tools:
        out["tools"] = tools
    if generation:
        out["generationConfig"] = generation
    return out


def upstream_for_gemini_provider(api: str, model: str, body: dict) -> dict:
    contents = []
    if api == "openai":
        system_parts = []
        for message in body.get("messages") or []:
            if not is_record(message):
                continue
            if message.get("role") == "system":
                text = blocks_text(message.get("content"))
                if text:
                    system_parts.append(text)
                continue
            role = "user" if message.get("role") == "user" else "model"
            content = message.get("content")
            texts = []
            images = []
            if isinstance(content, str):
                texts.append(content)
            elif isinstance(content, list):
                for block in content:
                    if not is_record(block):
                        continue
                    if block.get("type") == "text" and isinstance(block.get("text"), str):
                        texts.append(block["text"])
                    elif block.get("type") == "image_url" and is_record(block.get("image_url")):
                        mime, payload = data_url_parts(block["image_url"].get("url", ""))
                        if mime is not None:
                            images.append({"inlineData": {"mimeType": mime, "data": payload}})
                        elif payload:
                            images.append({"fileData": {"fileUri": payload}})
            parts = []
            if texts:
                parts.append({"text": "\n".join(texts)})
            parts.extend(images)
            contents.append({"role": role, "parts": parts})
        system_text = "\n".join(system_parts)
        tools = []
        for tool in body.get("tools") or []:
            if is_record(tool) and is_record(tool.get("function")):
                fn = tool["function"]
                declaration = {"name": fn.get("name"), "description": fn.get("description", "")}
                if "parameters" in fn:
                    declaration["parameters"] = fn["parameters"]
                tools.append({"functionDeclarations": [declaration]})
        generation: dict = {}
        max_tokens = body.get("max_tokens")
        if not isinstance(max_tokens, (int, float)):
            max_tokens = body.get("max_completion_tokens")
        if isinstance(max_tokens, (int, float)):
            generation["maxOutputTokens"] = max_tokens
        stops = string_list(body.get("stop"))
        if stops:
            generation["stopSequences"] = stops
        response_format = body.get("response_format")
        if is_record(response_format) and response_format.get("type") in ("json_object", "json_schema"):
            generation["responseMimeType"] = "application/json"
        for name in ("temperature", "top_p"):
            if name in body:
                generation[name] = body[name]
        return gemini_body_from_messages(contents, system_text, tools, generation)

    system_text = blocks_text(body.get("system"))
    for message in body.get("messages") or []:
        if not is_record(message):
            continue
        role = "user" if message.get("role") == "user" else "model"
        content = message.get("content")
        texts = []
        images = []
        if isinstance(content, str):
            texts.append(content)
        elif isinstance(content, list):
            for block in content:
                if not is_record(block):
                    continue
                if block.get("type") == "text" and isinstance(block.get("text"), str):
                    texts.append(block["text"])
                elif block.get("type") == "image" and is_record(block.get("source")):
                    source = block["source"]
                    if source.get("type") == "base64" and isinstance(source.get("data"), str):
                        images.append({"inlineData": {"mimeType": source.get("media_type", "image/png"), "data": source["data"]}})
                    elif source.get("type") == "url" and isinstance(source.get("url"), str):
                        images.append({"fileData": {"fileUri": source["url"]}})
        parts = []
        if texts:
            parts.append({"text": "\n".join(texts)})
        parts.extend(images)
        contents.append({"role": role, "parts": parts})
    tools = []
    for tool in body.get("tools") or []:
        if is_record(tool) and isinstance(tool.get("name"), str):
            tools.append(
                {
                    "functionDeclarations": [
                        {
                            "name": tool["name"],
                            "description": tool.get("description", ""),
                            "parameters": tool.get("input_schema") or {"type": "object"},
                        }
                    ]
                }
            )
    generation = {}
    if isinstance(body.get("max_tokens"), (int, float)):
        generation["maxOutputTokens"] = body["max_tokens"]
    stops = string_list(body.get("stop_sequences"))
    if stops:
        generation["stopSequences"] = stops
    for name in ("temperature", "top_p"):
        if name in body:
            generation[name] = body[name]
    return gemini_body_from_messages(contents, system_text, tools, generation)


def upstream_body_for(entry: dict, api: str, model: str, body: dict) -> dict:
    wire = entry.get("wire")
    if wire == "openai":
        return upstream_for_openai_provider(api, model, body)
    if wire == "gemini":
        return upstream_for_gemini_provider(api, model, body)
    if api == "openai":
        out = {"model": model, "stream": False, "max_tokens": body.get("max_tokens") if isinstance(body.get("max_tokens"), (int, float)) else 1024}
        messages = []
        system_parts = []
        for message in body.get("messages") or []:
            if not is_record(message):
                continue
            if message.get("role") == "system":
                text = blocks_text(message.get("content"))
                if text:
                    system_parts.append(text)
                continue
            content = message.get("content")
            blocks = []
            if isinstance(content, str):
                blocks.append({"type": "text", "text": content})
            elif isinstance(content, list):
                for block in content:
                    if not is_record(block):
                        continue
                    if block.get("type") == "text" and isinstance(block.get("text"), str):
                        blocks.append({"type": "text", "text": block["text"]})
                    elif block.get("type") == "image_url" and is_record(block.get("image_url")):
                        mime, payload = data_url_parts(block["image_url"].get("url", ""))
                        if mime is not None:
                            blocks.append({"type": "image", "source": {"type": "base64", "media_type": mime, "data": payload}})
                        elif payload:
                            blocks.append({"type": "image", "source": {"type": "url", "url": payload}})
            messages.append({"role": message.get("role", "user"), "content": blocks})
        if system_parts:
            out["system"] = "\n".join(system_parts)
        out["messages"] = messages
        tools = []
        for tool in body.get("tools") or []:
            if is_record(tool) and is_record(tool.get("function")):
                fn = tool["function"]
                out_tool = {"name": fn.get("name"), "description": fn.get("description", ""), "input_schema": fn.get("parameters") or {"type": "object"}}
                tools.append(out_tool)
        if tools:
            out["tools"] = tools
        stops = string_list(body.get("stop"))
        if stops:
            out["stop_sequences"] = stops
        if "temperature" in body:
            out["temperature"] = body["temperature"]
        if "top_p" in body:
            out["top_p"] = body["top_p"]
        return out
    out = dict(body)
    out["model"] = model
    out["stream"] = False
    return out


# ---------------------------------------------------------------- the response wires


def sse_frame(event, data) -> str:
    prefix = "" if event is None else f"event: {event}\n"
    return f"{prefix}data: {json.dumps(data)}\n\n"


def openai_completion_response(model: str, text: str, usage, stop_hint) -> str:
    prompt, completion = usage
    return json.dumps(
        {
            "id": "chatcmpl-aggregator",
            "object": "chat.completion",
            "created": FIXED_CREATED,
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": text},
                    "finish_reason": "stop",
                    "logprobs": None,
                }
            ],
            "usage": {
                "prompt_tokens": prompt,
                "completion_tokens": completion,
                "total_tokens": (prompt or 0) + (completion or 0),
            },
        }
    ) + "\n"


def openai_stream_response(model: str, text: str, usage) -> str:
    prompt, completion = usage
    tokens = {
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": (prompt or 0) + (completion or 0),
    }

    def chunk(delta, finish):
        return {
            "id": "chatcmpl-aggregator",
            "object": "chat.completion.chunk",
            "created": FIXED_CREATED,
            "model": model,
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
        }

    frames = [sse_frame(None, chunk({"role": "assistant", "content": ""}, None))]
    pieces = [text[at : at + SSE_DELTA_CHARS] for at in range(0, len(text), SSE_DELTA_CHARS)] or [""]
    for piece in pieces:
        frames.append(sse_frame(None, chunk({"content": piece}, None)))
    frames.append(sse_frame(None, chunk({}, "stop")))
    frames.append(sse_frame(None, {"id": "chatcmpl-aggregator", "object": "chat.completion.chunk", "created": FIXED_CREATED, "model": model, "choices": [], "usage": tokens}))
    frames.append("data: [DONE]\n\n")
    return "".join(frames)


def anthropic_message_response(model: str, text: str, usage) -> str:
    prompt, completion = usage
    return json.dumps(
        {
            "id": "msg_aggregator",
            "type": "message",
            "role": "assistant",
            "model": model,
            "content": [{"type": "text", "text": text}],
            "stop_reason": "end_turn",
            "stop_sequence": None,
            "usage": {"input_tokens": prompt, "output_tokens": completion},
        }
    ) + "\n"


def anthropic_stream_response(model: str, text: str, usage) -> str:
    prompt, completion = usage
    frames = [
        sse_frame(
            "message_start",
            {
                "type": "message_start",
                "message": {
                    "id": "msg_aggregator",
                    "type": "message",
                    "role": "assistant",
                    "model": model,
                    "content": [],
                    "stop_reason": None,
                    "stop_sequence": None,
                    "usage": {"input_tokens": prompt, "output_tokens": 0},
                },
            },
        ),
        sse_frame("content_block_start", {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}),
    ]
    pieces = [text[at : at + SSE_DELTA_CHARS] for at in range(0, len(text), SSE_DELTA_CHARS)] or [""]
    for piece in pieces:
        frames.append(sse_frame("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": piece}}))
    frames.append(sse_frame("content_block_stop", {"type": "content_block_stop", "index": 0}))
    frames.append(
        sse_frame(
            "message_delta",
            {"type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence": None}, "usage": {"output_tokens": completion}},
        )
    )
    frames.append(sse_frame("message_stop", {"type": "message_stop"}))
    return "".join(frames)


def error_body(api: str, status: int, message: str) -> str:
    if api == "anthropic":
        return json.dumps({"type": "error", "error": {"type": "api_error", "message": message}}) + "\n"
    return json.dumps({"error": {"message": message, "type": "api_error", "param": None, "code": None}}) + "\n"


# ---------------------------------------------------------------- serving


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        if length > MAX_BODY_BYTES:
            raise ValueError("body too large")
        return self.rfile.read(length) if length > 0 else b""

    def reply(self, status: int, payload: str, content_type: str = "application/json"):
        data = payload.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):  # noqa: N802
        path = urllib.parse.urlparse(self.path).path
        if path == "/v1/models":
            self.serve_models()
        else:
            self.reply(404, json.dumps({"error": {"message": f"no route {path}", "type": "invalid_request_error", "param": None, "code": "not_found"}}) + "\n")

    def do_POST(self):  # noqa: N802
        path = urllib.parse.urlparse(self.path).path
        if path == "/v1/chat/completions":
            self.serve_request("openai")
        elif path == "/v1/messages":
            self.serve_request("anthropic")
        else:
            body = self.read_body()
            self.reply(404, json.dumps({"error": {"message": f"no route {path}", "type": "invalid_request_error", "param": None, "code": "not_found"}}) + "\n")

    def serve_models(self):
        ids = free_model_ids()
        if "authorization" in {name.lower() for name in self.headers}:
            payload = json.dumps(
                {
                    "object": "list",
                    "data": [{"id": mid, "object": "model", "created": FIXED_CREATED, "owned_by": "aggregator", "free": True} for mid in ids],
                }
            ) + "\n"
        else:
            payload = json.dumps(
                {
                    "data": [{"type": "model", "id": mid, "display_name": mid, "created_at": "2026-01-01T00:00:00Z", "free": True} for mid in ids],
                    "has_more": False,
                }
            ) + "\n"
        self.reply(200, payload)

    def serve_request(self, api: str):
        index = router.next_request_index()
        manifest = brief.manifest
        request = manifest[index] if index < len(manifest) else {"id": f"x{index}", "requires": {}}
        requires = request.get("requires") or {}
        try:
            raw = self.read_body()
            body = json.loads(raw.decode("utf-8")) if raw else {}
            if not is_record(body):
                raise ValueError("the request body is not one JSON object")
        except Exception as err:  # noqa: BLE001
            self.reply(400, error_body(api, 400, str(err)))
            return
        stream = body.get("stream") is True
        candidates = router.candidates(api, requires)
        if not candidates:
            self.reply(503, error_body(api, 503, "no free model meets this request's requirements"))
            return
        last_error = "no provider answered"
        attempts = []
        for provider, model in candidates:
            if not router.usable(provider, index):
                continue
            entry = next((e for e in brief.providers if e.get("id") == provider), None)
            if entry is None:
                continue
            upstream = upstream_body_for(entry, api, model, body)
            try:
                text, usage, stop_hint = call_provider(entry, model, upstream)
            except ProviderRefused as err:
                last_error = f"{provider} refused with {err.status} {err.code}"
                attempts.append(f"{provider}/{model}: refused {err.status} {err.code}")
                if err.code != "concurrent_requests":
                    router.note_refusal(provider, index)
                continue
            except ProviderError as err:
                last_error = str(err)
                attempts.append(f"{provider}/{model}: error {err}")
                continue
            router.note_success(provider)
            attempts.append(f"{provider}/{model}: ok")
            if api == "openai":
                payload = openai_stream_response(model, text, usage) if stream else openai_completion_response(model, text, usage, stop_hint)
            else:
                payload = anthropic_stream_response(model, text, usage) if stream else anthropic_message_response(model, text, usage)
            self.reply(200, payload, "text/event-stream" if stream else "application/json")
            log(f"{request.get('id')} -> {provider}/{model} ({'; '.join(attempts)})")
            return
        log(f"{request.get('id')} -> FAILED ({'; '.join(attempts)})")
        self.reply(502, error_body(api, 502, last_error))


# ---------------------------------------------------------------- the env client


def register(port: int, catalog: dict) -> None:
    if not ENV_URL:
        return
    body = {"protocol_version": PROTOCOL_VERSION, "port": port, "catalog": catalog}
    for attempt in range(30):
        try:
            status, payload = http_json("POST", f"{ENV_URL}/register", {}, body, timeout=10)
            if 200 <= status < 300:
                log(f"registered port {port}: {payload.strip()[:120]}")
                return
            log(f"register answered {status}: {payload.strip()[:200]}")
        except Exception as err:  # noqa: BLE001
            log(f"register failed: {err}")
    log("register never succeeded; serving anyway")


def poll_work() -> None:
    """Exit 0 as soon as the env says the workload is done."""
    if not ENV_URL:
        return
    while True:
        try:
            status, payload = http_json("GET", f"{ENV_URL}/work", {}, timeout=10)
            if 200 <= status < 300:
                value = json.loads(payload)
                if is_record(value) and value.get("done") is True:
                    log("the workload is done; exiting 0")
                    sys.stdout.flush()
                    os._exit(0)
        except Exception:  # noqa: BLE001
            pass


def main() -> int:
    if "--list" in sys.argv:
        print(CATALOG_TEXT if brief.error is None else json.dumps({"providers": []}))
        return 0
    if brief.error:
        log(brief.error)
    server = ThreadingHTTPServer(("0.0.0.0", 0), Handler)
    server.daemon_threads = True
    port = server.server_address[1]
    log(f"listening on port {port}")
    register(port, CATALOG)
    threading.Thread(target=poll_work, daemon=True).start()
    server.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
