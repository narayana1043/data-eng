import os
import re
import json
import time
import logging

import asyncpg
import sqlparse
from mcp.server.fastmcp import FastMCP

# ---------------- Logging ----------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

def setup_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s.%(msecs)03d | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

log = logging.getLogger("mcp-server")

mcp = FastMCP("dvdrental-mcp")

DB_DSN = os.getenv("DB_DSN", "postgresql://airflow:airflow@postgres:5432/dvdrental")

# ---- safety rails (keep internal + read-only) ----
FORBIDDEN = re.compile(r"\b(insert|update|delete|drop|alter|truncate|create|grant|revoke)\b", re.I)

def enforce_read_only(sql: str) -> str:
    parsed = sqlparse.format(sql, strip_comments=True).strip()
    if not parsed:
        raise ValueError("Empty SQL")
    if FORBIDDEN.search(parsed):
        raise ValueError("Write operations are blocked (read-only server).")
    return parsed

def clamp_limit(sql: str, max_rows: int = 200) -> str:
    upper = sql.upper()
    if "LIMIT" not in upper:
        return f"{sql}\nLIMIT {max_rows}"
    return sql

async def get_conn():
    # Avoid logging full DSN (it may include creds)
    log.debug("Opening DB connection")
    return await asyncpg.connect(DB_DSN)

# ---- tools ----
@mcp.tool()
async def list_tables(schema: str = "public") -> dict:
    """List tables in a schema."""
    log.info("tool=list_tables schema=%s", schema)
    conn = await get_conn()
    try:
        t0 = time.perf_counter()
        rows = await conn.fetch(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = $1 AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            schema,
        )
        dt = (time.perf_counter() - t0) * 1000
        tables = [r["table_name"] for r in rows]
        log.info("list_tables done time_ms=%.1f count=%d", dt, len(tables))
        return {"schema": schema, "tables": tables}
    finally:
        await conn.close()
        log.debug("DB connection closed")

@mcp.tool()
async def describe_table(table: str, schema: str = "public") -> dict:
    """Describe columns for a table."""
    log.info("tool=describe_table schema=%s table=%s", schema, table)
    conn = await get_conn()
    try:
        t0 = time.perf_counter()
        rows = await conn.fetch(
            """
            SELECT
              column_name, data_type, is_nullable, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
            """,
            schema, table,
        )
        dt = (time.perf_counter() - t0) * 1000
        cols = [
            {
                "name": r["column_name"],
                "type": r["data_type"],
                "nullable": (r["is_nullable"] == "YES"),
                "max_len": r["character_maximum_length"],
            }
            for r in rows
        ]
        log.info("describe_table done time_ms=%.1f columns=%d", dt, len(cols))
        return {"schema": schema, "table": table, "columns": cols}
    finally:
        await conn.close()
        log.debug("DB connection closed")

@mcp.tool()
async def run_query(sql: str, max_rows: int = 200) -> dict:
    """
    Execute a safe read-only SQL query.
    - Blocks writes/DDL
    - Adds LIMIT if missing
    """
    log.info("tool=run_query max_rows=%d", max_rows)

    sql_clean = enforce_read_only(sql)
    sql_limited = clamp_limit(sql_clean, max_rows=max_rows)

    # Log SQL (safe enough because it’s read-only, but keep it at DEBUG if preferred)
    if log.isEnabledFor(logging.INFO):
        log.info("SQL (limited): %s", " ".join(sql_limited.split())[:1200])

    conn = await get_conn()
    try:
        t0 = time.perf_counter()
        rows = await conn.fetch(sql_limited)
        dt = (time.perf_counter() - t0) * 1000

        data = [dict(r) for r in rows]
        log.info("run_query done time_ms=%.1f row_count=%d", dt, len(data))

        if log.isEnabledFor(logging.DEBUG):
            log.debug("First row: %s", json.dumps(data[0], default=str) if data else "None")

        return {"sql": sql_limited, "row_count": len(data), "rows": data}
    finally:
        await conn.close()
        log.debug("DB connection closed")

if __name__ == "__main__":
    setup_logging()
    log.info("Starting dvdrental-mcp server (stdio). LOG_LEVEL=%s", LOG_LEVEL)
    mcp.run()
