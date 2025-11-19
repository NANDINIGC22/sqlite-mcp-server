import os
import sqlite3
from mcp.server.fastmcp import FastMCP

# ============================================================
# Initialize Server (Compatible with new MCP runtime)
# ============================================================

mcp = FastMCP("sqlite-dynamic-mcp")


# ============================================================
# Helper Utilities
# ============================================================

# Absolute path to project directory (server.py location)
#BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = r"C:\AIProjects\MCPPROJECTS"

DB_DIR = os.path.join(BASE_DIR, "databases")
os.makedirs(DB_DIR, exist_ok=True)

def db_path(name: str):
    return os.path.join(DB_DIR, f"{name}.db")


def exec_sql(db: str, query: str, params=(), fetch=False):
    conn = sqlite3.connect(db)
    cur = conn.cursor()

    try:
        cur.execute(query, params)

        if fetch:
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            return {"columns": cols, "rows": rows}

        conn.commit()
        return {"status": "success"}

    except Exception as e:
        return {"error": str(e)}

    finally:
        conn.close()


def nl_to_sql(prompt: str, table: str):
    prompt = prompt.lower()

    if "all" in prompt or "show" in prompt or "list" in prompt:
        return f"SELECT * FROM {table};"

    if "count" in prompt:
        return f"SELECT COUNT(*) AS count FROM {table};"

    return f"SELECT * FROM {table};"


# ============================================================
# TOOL 1: Create Database
# ============================================================

@mcp.tool()
def create_database(db_name: str):
    """Create a new SQLite database dynamically."""

    path = db_path(db_name)

    if os.path.exists(path):
        return {"message": f"Database '{db_name}' already exists"}

    sqlite3.connect(path).close()

    return {"message": f"Database '{db_name}' created successfully!"}


# ============================================================
# TOOL 2: Create Table
# ============================================================

@mcp.tool()
def create_table(db_name: str, table_name: str, columns: list):
    """Create a dynamic table."""

    db = db_path(db_name)

    if not os.path.exists(db):
        return {"error": f"Database '{db_name}' does not exist"}

    col_defs = ", ".join([f"{c['name']} {c['type']}" for c in columns])
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs});"

    exec_sql(db, sql)

    return {"message": f"Table '{table_name}' created."}


# ============================================================
# TOOL 3: Insert Records
# ============================================================

@mcp.tool()
def insert_records(db_name: str, table_name: str, records: list):
    """Insert rows dynamically."""

    db = db_path(db_name)

    if not os.path.exists(db):
        return {"error": f"Database '{db_name}' does not exist"}

    if not records:
        return {"error": "No records provided"}

    keys = records[0].keys()
    columns = ", ".join(keys)
    placeholders = ", ".join(["?"] * len(keys))
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    conn = sqlite3.connect(db)
    cur = conn.cursor()

    try:
        for row in records:
            cur.execute(sql, tuple(row.values()))
        conn.commit()
    except Exception as e:
        return {"error": str(e)}
    finally:
        conn.close()

    return {"message": f"{len(records)} records inserted."}


# ============================================================
# TOOL 4: Natural-Language Query
# ============================================================

@mcp.tool()
def prompt_query(db_name: str, table_name: str, prompt: str):
    """Ask a plain-language question; tool converts to SQL."""

    db = db_path(db_name)

    if not os.path.exists(db):
        return {"error": f"Database '{db_name}' does not exist"}

    sql = nl_to_sql(prompt, table_name)

    return exec_sql(db, sql, fetch=True)


# ============================================================
# Run Server
# ============================================================

if __name__ == "__main__":
    mcp.run()
