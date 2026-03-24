from __future__ import annotations
 
import sys
from pathlib import Path
 
# Ensure project root is on PYTHONPATH
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
 
from app.services.snowflake import get_snowflake_connection
 
 
def split_sql_statements(sql: str) -> list[str]:
    """
    Splits SQL script into individual statements.
    Assumes statements are separated by semicolons.
    Safe for plain DDL + MERGE (no stored procedures).
    """
    parts = [p.strip() for p in sql.split(";")]
    return [p for p in parts if p]
 
 
def strip_leading_line_comments(sql: str) -> str:
    """
    Removes leading '--' comment lines so Snowflake
    does not choke when executing statements.
    """
    lines = sql.splitlines()
    i = 0
    while i < len(lines) and lines[i].lstrip().startswith("--"):
        i += 1
    return "\n".join(lines[i:]).strip()
 
 
def main() -> int:
    schema_path = ROOT / "app" / "database" / "schema.sql"
 
    if not schema_path.exists():
        raise SystemExit(f"❌ schema.sql not found at: {schema_path.resolve()}")
 
    print(f"📄 Applying schema from: {schema_path}")
 
    sql_text = schema_path.read_text(encoding="utf-8", errors="ignore")
    statements = split_sql_statements(sql_text)
 
    conn = get_snowflake_connection()
    cur = conn.cursor()
 
    try:
        applied = 0
 
        for i, stmt in enumerate(statements, start=1):
            s = strip_leading_line_comments(stmt)
            if not s:
                continue
 
            try:
                cur.execute(s)
                applied += 1
            except Exception as e:
                print(f"\n❌ Failed on statement #{i}")
                print("--------------------------------------------------")
                print(s)
                print("--------------------------------------------------")
                raise e
 
        # Explicit commit (safe even if Snowflake auto-commits DDL)
        try:
            conn.commit()
        except Exception:
            pass
 
        print(f"\n✅ Successfully applied {applied} SQL statements.")
        return 0
 
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
 
 
if __name__ == "__main__":
    raise SystemExit(main())