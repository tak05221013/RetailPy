import os
import json
import time
import secrets
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Form, Header, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel, Field
import pymysql

load_dotenv("/home/retail/py/env/asin-to-remember.env")

API_KEY = os.environ.get("MC_LOG_API_KEY", "golden")
MYSQL_HOST = os.environ.get("MC_MYSQL_HOST", "192.168.1.1")
MYSQL_PORT = int(os.environ.get("MC_MYSQL_PORT", "3306"))
MYSQL_USER = os.environ.get("MC_MYSQL_USER", "retail")
MYSQL_PASS = os.environ.get("MC_MYSQL_PASS", "retailre2021")
MYSQL_DB   = os.environ.get("MC_MYSQL_DB", "retail")
JANCODE_MST_PATH = os.environ.get("MC_JANCODE_MST_PATH", "/home/retail/mst/map.jancode.mst")
ASIN_AUTH_USER = os.environ.get("ASIN_TO_REMEMBER_USER")
ASIN_AUTH_PASS = os.environ.get("ASIN_TO_REMEMBER_PASS")

basic_security = HTTPBasic()

app = FastAPI(title="MapCamera Log Ingest")

ASIN_FORM_HTML = """
<!DOCTYPE html>
<html lang="ja">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>ASIN 登録</title>
    <style>
      body {
        font-family: "Helvetica Neue", Arial, sans-serif;
        background: #f6f7fb;
        margin: 0;
        padding: 40px 16px;
        color: #1f2937;
      }
      .card {
        max-width: 480px;
        margin: 0 auto;
        background: #fff;
        border-radius: 12px;
        box-shadow: 0 12px 30px rgba(15, 23, 42, 0.08);
        padding: 32px;
      }
      h1 {
        font-size: 20px;
        margin: 0 0 20px;
      }
      label {
        display: block;
        font-size: 14px;
        margin-bottom: 8px;
      }
      input[type="text"] {
        width: 100%;
        padding: 12px 14px;
        border-radius: 8px;
        border: 1px solid #d1d5db;
        font-size: 16px;
      }
      button {
        margin-top: 16px;
        background: #2563eb;
        color: #fff;
        border: none;
        padding: 12px 18px;
        border-radius: 8px;
        font-size: 15px;
        cursor: pointer;
      }
      button:disabled {
        opacity: 0.6;
        cursor: not-allowed;
      }
      #status {
        margin-top: 16px;
        font-size: 14px;
      }
    </style>
  </head>
  <body>
    <div class="card">
      <h1>ASIN 登録フォーム</h1>
      <form id="asin-form">
        <label for="asin">ASIN (10 文字)</label>
        <input id="asin" name="asin" type="text" maxlength="10" required />
        <button type="submit">登録</button>
      </form>
      <div id="status"></div>
    </div>
    <script>
      const form = document.getElementById("asin-form");
      const statusEl = document.getElementById("status");
      const params = new URLSearchParams(window.location.search);
      const asinFromQuery = params.get("asin");
      if (asinFromQuery) {
        form.asin.value = asinFromQuery;
      }
      form.addEventListener("submit", async (event) => {
        event.preventDefault();
        statusEl.textContent = "";
        const asin = form.asin.value.trim();
        if (!asin) {
          statusEl.textContent = "ASIN を入力してください。";
          return;
        }
        form.querySelector("button").disabled = true;
        try {
          const response = await fetch("/asin-to-remember", {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded" },
            body: new URLSearchParams({ asin }),
          });
          if (!response.ok) {
            const error = await response.json();
            throw new Error(error.detail || "登録に失敗しました。");
          }
          const result = await response.json();
          statusEl.textContent = `登録しました: ${result.asin}`;
          form.reset();
        } catch (error) {
          statusEl.textContent = error.message;
        } finally {
          form.querySelector("button").disabled = false;
        }
      });
    </script>
  </body>
</html>
"""

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

def require_asin_auth(credentials: HTTPBasicCredentials):
    if not ASIN_AUTH_USER or not ASIN_AUTH_PASS:
        raise HTTPException(status_code=500, detail="ASIN auth is not configured")
    is_valid_user = secrets.compare_digest(credentials.username, ASIN_AUTH_USER)
    is_valid_pass = secrets.compare_digest(credentials.password, ASIN_AUTH_PASS)
    if not (is_valid_user and is_valid_pass):
        raise HTTPException(
            status_code=401,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
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

class MapCameraDoc(BaseModel):
    genpin_id: int
    genpin_name: Optional[str] = None
    jancode: Optional[str] = None
    mapcode: Optional[str] = None
    maker_name_kana: Optional[str] = None
    salesprice: Optional[int] = None
    specialprice: Optional[int] = None
    selltypeid: Optional[int] = None
    conditionid: Optional[int] = None
    sellstatusid: Optional[int] = None
    pricedownflag: Optional[int] = None
    recommendflag: Optional[int] = None
    econlyflag: Optional[int] = None
    newstockflag: Optional[int] = None
    limitedflag: Optional[int] = None
    newproductflag: Optional[int] = None
    raremodelflag: Optional[int] = None
    beginnerflag: Optional[int] = None
    businessflag: Optional[int] = None
    reviewcount: Optional[int] = None
    reviewrating: Optional[float] = None
    point: Optional[int] = None
    subtitle: Optional[str] = None
    usednum: Optional[int] = None
    usedsalespricemin: Optional[int] = None
    usedsalespointmin: Optional[int] = None
    accessories: Optional[str] = None
    category_name: Optional[str] = None
    bestbadgeflag: Optional[str] = None
    usedconditionrank: Optional[str] = None
    logisticstockdispkbn: Optional[int] = None
    videoflag: Optional[int] = None

    class Config:
        extra = "ignore"

class DocsIn(BaseModel):
    docs: List[MapCameraDoc]
    client_ts_ms: Optional[int] = None
    page_url: Optional[str] = None
    context: Optional[str] = Field(default=None, max_length=16)

class DocDetailIn(BaseModel):
    jan: str = Field(..., max_length=20)
    genpinId: str = Field(..., max_length=50)
    price: Optional[int] = None
    cond: Optional[int] = None
    dsc: Optional[str] = None
    unixtime: Optional[int] = None
    date: Optional[str] = Field(default=None, max_length=10)
    time: Optional[str] = Field(default=None, max_length=8)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/asin-to-remember", response_class=HTMLResponse)
def asin_to_remember_form(credentials: HTTPBasicCredentials = Depends(basic_security)):
    require_asin_auth(credentials)
    return ASIN_FORM_HTML

@app.post("/asin-to-remember")
def save_asin_to_remember(
    asin: str = Form(..., max_length=10),
    credentials: HTTPBasicCredentials = Depends(basic_security),
):
    require_asin_auth(credentials)
    now_ms = int(time.time() * 1000)
    sql = """
    INSERT INTO asin_to_remember (asin, count, lastUpdateTime)
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE
     count=VALUES(count),
     lastUpdateTime=VALUES(lastUpdateTime)
    """

    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(sql, (asin, 0, now_ms))
        return {"asin": asin, "lastUpdateTime": now_ms}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            conn.close()
        except Exception:
            pass

@app.get("/mapcamera-jancode-mst")
def get_jancode_mst(x_api_key: str = Header(default="")):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        with open(JANCODE_MST_PATH, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="jancode mst not found") from exc
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=500, detail="invalid jancode mst json") from exc

@app.post("/ingest")
def ingest(payload: BatchIn, x_api_key: str = Header(default="")):
    if API_KEY and x_api_key != API_KEY:
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

@app.post("/mapcamera-search-docs")
def ingest_docs(payload: DocsIn, x_api_key: str = Header(default="")):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if not payload.docs:
        return {"inserted": 0}

    updatetime = payload.client_ts_ms or int(time.time() * 1000)
    rows = []
    for doc in payload.docs:
        rows.append((
            doc.genpin_id,
            doc.genpin_name,
            doc.jancode,
            doc.mapcode,
            doc.maker_name_kana,
            doc.salesprice,
            doc.specialprice,
            doc.selltypeid,
            doc.conditionid,
            doc.sellstatusid,
            doc.pricedownflag,
            doc.recommendflag,
            doc.econlyflag,
            doc.newstockflag,
            doc.limitedflag,
            doc.newproductflag,
            doc.raremodelflag,
            doc.beginnerflag,
            doc.businessflag,
            doc.reviewcount,
            doc.reviewrating,
            doc.point,
            doc.subtitle,
            doc.usednum,
            doc.usedsalespricemin,
            doc.usedsalespointmin,
            doc.accessories,
            doc.category_name,
            doc.bestbadgeflag,
            doc.usedconditionrank,
            doc.logisticstockdispkbn,
            doc.videoflag,
            updatetime,
        ))

    sql = """
    INSERT INTO mapcamera_search_docs
    (genpin_id, genpin_name, jancode, mapcode, maker_name_kana, salesprice, specialprice, selltypeid,
     conditionid, sellstatusid, pricedownflag, recommendflag, econlyflag, newstockflag, limitedflag,
     newproductflag, raremodelflag, beginnerflag, businessflag, reviewcount, reviewrating, point,
     subtitle, usednum, usedsalespricemin, usedsalespointmin, accessories, category_name, bestbadgeflag,
     usedconditionrank, logisticstockdispkbn, videoflag, updatetime)
    VALUES
    (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
     genpin_name=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(genpin_name),
        genpin_name
     ),
     jancode=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(jancode),
        jancode
     ),
     mapcode=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(mapcode),
        mapcode
     ),
     maker_name_kana=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(maker_name_kana),
        maker_name_kana
     ),
     salesprice=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(salesprice),
        salesprice
     ),
     specialprice=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(specialprice),
        specialprice
     ),
     selltypeid=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(selltypeid),
        selltypeid
     ),
     conditionid=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(conditionid),
        conditionid
     ),
     sellstatusid=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(sellstatusid),
        sellstatusid
     ),
     pricedownflag=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(pricedownflag),
        pricedownflag
     ),
     recommendflag=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(recommendflag),
        recommendflag
     ),
     econlyflag=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(econlyflag),
        econlyflag
     ),
     newstockflag=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(newstockflag),
        newstockflag
     ),
     limitedflag=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(limitedflag),
        limitedflag
     ),
     newproductflag=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(newproductflag),
        newproductflag
     ),
     raremodelflag=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(raremodelflag),
        raremodelflag
     ),
     beginnerflag=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(beginnerflag),
        beginnerflag
     ),
     businessflag=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(businessflag),
        businessflag
     ),
     reviewcount=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(reviewcount),
        reviewcount
     ),
     reviewrating=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(reviewrating),
        reviewrating
     ),
     point=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(point),
        point
     ),
     subtitle=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(subtitle),
        subtitle
     ),
     usednum=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(usednum),
        usednum
     ),
     usedsalespricemin=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(usedsalespricemin),
        usedsalespricemin
     ),
     usedsalespointmin=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(usedsalespointmin),
        usedsalespointmin
     ),
     accessories=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(accessories),
        accessories
     ),
     category_name=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(category_name),
        category_name
     ),
     bestbadgeflag=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(bestbadgeflag),
        bestbadgeflag
     ),
     usedconditionrank=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(usedconditionrank),
        usedconditionrank
     ),
     logisticstockdispkbn=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(logisticstockdispkbn),
        logisticstockdispkbn
     ),
     videoflag=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(videoflag),
        videoflag
     ),
     updatetime=IF(
        NOT (VALUES(jancode) <=> jancode)
        OR NOT (VALUES(salesprice) <=> salesprice)
        OR NOT (VALUES(specialprice) <=> specialprice),
        VALUES(updatetime),
        updatetime
     )
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

@app.post("/mapcamera-doc-detail")
def ingest_doc_detail(payload: DocDetailIn, x_api_key: str = Header(default="")):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

    sql = """
    INSERT INTO mapproduct_new_desc
    (jan, genpinId, price, cond, dsc, unixtime, date, time)
    VALUES
    (%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
     price=VALUES(price),
     cond=VALUES(cond),
     dsc=VALUES(dsc),
     unixtime=VALUES(unixtime),
     date=VALUES(date),
     time=VALUES(time)
    """

    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    payload.jan,
                    payload.genpinId,
                    payload.price,
                    payload.cond,
                    payload.dsc,
                    payload.unixtime,
                    payload.date,
                    payload.time,
                ),
            )
        return {"inserted": 1}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            conn.close()
        except Exception:
            pass
