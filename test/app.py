import os
import json
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field
import pymysql

API_KEY = os.environ.get("MC_LOG_API_KEY", "")
MYSQL_HOST = os.environ.get("MC_MYSQL_HOST", "192.168.1.1")
MYSQL_PORT = int(os.environ.get("MC_MYSQL_PORT", "3306"))
MYSQL_USER = os.environ.get("MC_MYSQL_USER", "retail")
MYSQL_PASS = os.environ.get("MC_MYSQL_PASS", "retailre2021")
MYSQL_DB   = os.environ.get("MC_MYSQL_DB", "retail")

app = FastAPI(title="MapCamera Log Ingest")

def get_conn():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        database=MYSQL_DB,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

def _to_json_or_text(value: Any):
    """
    value が dict/list/number/bool/null のように JSON として保存できるなら json列へ。
    それ以外（長い文字列等）は text列へ。
    """
    if value is None:
        return (None, None)
    if isinstance(value, (dict, list, int, float, bool)):
        return (json.dumps(value, ensure_ascii=False), None)
    if isinstance(value, str):
        # JSON文字列っぽければ parse 試す（任意）
        try:
            parsed = json.loads(value)
            if isinstance(parsed, (dict, list)):
                return (json.dumps(parsed, ensure_ascii=False), None)
        except Exception:
            pass
        return (None, value)
    # その他は文字列化
    return (None, str(value))

class LogItem(BaseModel):
    client_ts_ms: Optional[int] = None
    session_id: Optional[str] = Field(default=None, max_length=64)
    trace_id: Optional[str] = Field(default=None, max_length=64)

    page_url: Optional[str] = None
    context: Optional[str] = Field(default=None, max_length=16)  # xhr/fetch
    method: Optional[str] = Field(default=None, max_length=16)
    url: Optional[str] = None
    status: Optional[int] = None
    content_type: Optional[str] = Field(default=None, max_length=255)

    request_body: Optional[Any] = None
    response_body: Optional[Any] = None

class BatchIn(BaseModel):
    items: List[LogItem]

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/ingest")
def ingest(payload: BatchIn, x_api_key: str = Header(default="")):
    if not API_KEY or x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

    rows = []
    for it in payload.items:
        req_json, req_text = _to_json_or_text(it.request_body)
        res_json, res_text = _to_json_or_text(it.response_body)
        rows.append((
            it.client_ts_ms,
            it.session_id,
            it.trace_id,
            it.page_url,
            it.context,
            it.method,
            it.url,
            it.status,
            it.content_type,
            req_json,
            res_json,
            req_text,
            res_text,
        ))

    sql = """
    INSERT INTO itemsearch_logs
    (client_ts_ms, session_id, trace_id, page_url, context, method, url, status, content_type,
     request_body_json, response_body_json, request_body_text, response_body_text)
    VALUES
    (%s,%s,%s,%s,%s,%s,%s,%s,%s,
     CAST(%s AS JSON), CAST(%s AS JSON), %s, %s)
    """

    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        return {"inserted": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            conn.close()
        except Exception:
            pass
