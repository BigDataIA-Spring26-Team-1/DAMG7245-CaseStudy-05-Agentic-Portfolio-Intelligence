import os
from contextlib import contextmanager

from app.config import settings
 
_snowflake_import_error: Exception | None = None
try:
    import snowflake.connector as snowflake_connector
except Exception as exc:
    snowflake_connector = None
    _snowflake_import_error = exc
 
 
def get_snowflake_connection():
    if snowflake_connector is None:
        raise RuntimeError("snowflake-connector-python is not installed or failed to import") from _snowflake_import_error

    if not settings.snowflake_account or not settings.snowflake_user or not settings.snowflake_password:
        raise RuntimeError("Snowflake credentials missing (SNOWFLAKE_ACCOUNT/USER/PASSWORD)")
 
    with _without_bad_local_proxy():
        return snowflake_connector.connect(
            account=settings.snowflake_account,
            user=settings.snowflake_user,
            password=settings.snowflake_password,
            warehouse=settings.snowflake_warehouse,
            database=settings.snowflake_database,
            schema=settings.snowflake_schema,
            role=settings.snowflake_role,
        )


@contextmanager
def _without_bad_local_proxy():
    """
    Ignore the known broken loopback proxy (127.0.0.1:9) injected in some local
    environments. Keep any real proxy values intact.
    """
    keys = ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]
    removed: dict[str, str] = {}
    try:
        for key in keys:
            value = os.environ.get(key)
            if value and "127.0.0.1:9" in value:
                removed[key] = value
                os.environ.pop(key, None)
        yield
    finally:
        for key, value in removed.items():
            os.environ[key] = value
 
 
def ping_snowflake() -> tuple[bool, str]:
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1")
            cur.fetchone()
            return True, "ok"
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"
