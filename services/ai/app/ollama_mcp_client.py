import asyncio
import json
import os, sys
import time
import logging
from typing import Any, Dict, List

import httpx
from mcp.client.stdio import stdio_client, StdioServerParameters
from mcp import ClientSession

# ---------------- Logging ----------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

def setup_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s.%(msecs)03d | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

log = logging.getLogger("mcp-client")

# ---- Config ----
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:3b")  # change to what you pulled
SERVER_CMD = StdioServerParameters(
    command=sys.executable,                 # "python"
    args=["/app/server.py"],                # path to your server.py inside container
    env={**os.environ},  # pass DB_DSN
)

MAX_TURNS = 15

SYSTEM_PROMPT = """
You are a helpful data assistant for the dvdrental PostgreSQL database.

You can use these tools by returning a JSON object (and only JSON) in this format:
{"tool": "<tool_name>", "arguments": { ... }}

Available tools:
- list_tables(schema="public") -> returns tables
- describe_table(table, schema="public") -> returns columns
- run_query(sql, max_rows=200) -> executes read-only SQL (writes/DDL are blocked)

Rules:
- If you need schema info, call list_tables/describe_table first. Always use public schema
- Prefer explicit joins. Keep queries efficient (use LIMIT).
- After you have enough info and results, respond with:
- Use only tables/columns discovered via list_tables and describe_table. Do not invent names. Never use RETURNING clauses.
{"final": "<clear answer for the user>"}

Do not output anything except the JSON object.
""".strip()


async def ollama_chat(messages: List[Dict[str, str]]) -> str:
    """
    Calls Ollama /api/chat with stream=false and returns assistant 'content'.
    """
    payload = {
        "model": OLLAMA_MODEL,
        "stream": False,
        "messages": messages,
        "options": {"num_ctx": 2048},
    }

    # Helpful debug summary (don’t dump full content unless DEBUG)
    log.debug("Ollama payload: model=%s messages=%d", OLLAMA_MODEL, len(messages))
    if log.isEnabledFor(logging.DEBUG):
        last = messages[-1]
        log.debug("Last message: role=%s content=%s", last.get("role"), (last.get("content") or "")[:500])

    t0 = time.perf_counter()
    async with httpx.AsyncClient(timeout=120) as client:
        r = await client.post(
            f"{OLLAMA_BASE_URL}/api/chat",
            headers={"Content-Type": "application/json"},
            json=payload,
        )
    dt = (time.perf_counter() - t0) * 1000

    if r.status_code != 200:
        log.error("Ollama error status=%s time_ms=%.1f body=%s", r.status_code, dt, r.text[:800])
        raise RuntimeError(f"Ollama error {r.status_code}: {r.text}")

    data = r.json()
    content = data["message"]["content"]
    log.info("Ollama response time_ms=%.1f chars=%d", dt, len(content))
    if log.isEnabledFor(logging.DEBUG):
        log.debug("Ollama raw text: %s", content)

    return content


def extract_json(text: str) -> Dict[str, Any]:
    """
    Tries to parse the model response as JSON.
    If the model wrapped JSON with extra text, this attempts a best-effort extraction.
    """
    text = text.strip()
    try:
        return json.loads(text)
    except Exception:
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidate = text[start : end + 1]
        return json.loads(candidate)

    raise ValueError(f"Model did not return valid JSON. Got:\n{text}")


async def get_action_from_model(messages: List[Dict[str, str]]) -> Dict[str, Any]:
    for attempt in range(1,7):  # retry up to 6 times
        log.info("Model action request attempt=%d", attempt)
        text = await ollama_chat(messages)
        try:
            action = extract_json(text)
            log.info("Model action parsed keys=%s", list(action.keys()))
            if log.isEnabledFor(logging.DEBUG):
                log.debug("Parsed action: %s", json.dumps(action, indent=2))
            return action
        except Exception as e:
            log.warning("Model returned non-JSON (attempt=%d). error=%s", attempt, str(e))
            messages.append({"role": "assistant", "content": text})
            messages.append({
                "role": "user",
                "content": """
You did not return valid JSON.

Return ONLY a JSON object in one of these forms:

Tool call:
{"tool": "tool_name", "arguments": {...}}

Final answer:
{"final": "answer"}

Do not write anything else.
""".strip()
            })
    raise RuntimeError("Model failed to produce JSON after retries.")


async def run():
    user_question = input("Ask dvdrental: ").strip()
    if not user_question:
        print("No question.")
        return

    log.info("User question: %s", user_question)

    # Start MCP session (stdio)
    log.info("Starting MCP stdio session: cmd=%s args=%s", SERVER_CMD.command, SERVER_CMD.args)

    async with stdio_client(SERVER_CMD) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            log.info("MCP session initialized")

            tools = await session.list_tools()
            available = [t.name for t in tools.tools]
            log.info("Tools discovered: %s", ", ".join(available))

            messages: List[Dict[str, str]] = [
                {"role": "system", "content": SYSTEM_PROMPT + "\n\nTools discovered: " + ", ".join(available)},
                {"role": "user", "content": user_question},
            ]

            for turn in range(1, MAX_TURNS + 1):
                log.info("---- TURN %d/%d ----", turn, MAX_TURNS)
                action = await get_action_from_model(messages)

                if "final" in action:
                    log.info("Final received from model")
                    print("\n=== Final ===")
                    print(action["final"])
                    return

                tool = action.get("tool")
                args = action.get("arguments", {})

                log.info("Calling tool: %s args=%s", tool, args)

                t0 = time.perf_counter()
                result = await session.call_tool(tool, args)
                dt = (time.perf_counter() - t0) * 1000

                tool_out = {
                    "tool": tool,
                    "arguments": args,
                    "result": [getattr(c, "text", str(c)) for c in result.content],
                }

                log.info("Tool returned: %s time_ms=%.1f output_items=%d", tool, dt, len(tool_out["result"]))
                if log.isEnabledFor(logging.DEBUG):
                    log.debug("Tool output: %s", json.dumps(tool_out, indent=2)[:4000])

                # If run_query returned rows -> force summarization
                if tool == "run_query":
                    log.info("run_query executed. Forcing summarization and stopping tool loop.")
                    messages.append({"role": "assistant", "content": json.dumps(action)})
                    messages.append({
                        "role": "user",
                        "content": f"""
You now have the query results below.

Write a FINAL human-readable answer for the user.
Do NOT call any more tools.

Results:
{json.dumps(tool_out, indent=2)}
""".strip()
                    })

                    final_text = await ollama_chat(messages)
                    print("\n=== Final ===")
                    print(final_text)
                    return

                # otherwise continue normal loop
                messages.append({"role": "assistant", "content": json.dumps(action)})
                messages.append({"role": "user", "content": f"Tool output:\n{json.dumps(tool_out, indent=2)}"})

            log.warning("Stopped: max turns reached without a final answer.")
            print("\nStopped: max turns reached without a final answer.")


if __name__ == "__main__":
    setup_logging()
    asyncio.run(run())
