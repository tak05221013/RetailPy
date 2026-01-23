import os
import json
import time
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

@app.post("/mapcamera-search-docs")
def ingest_docs(payload: DocsIn, x_api_key: str = Header(default="")):
    if not API_KEY or x_api_key != API_KEY:
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
     genpin_name=VALUES(genpin_name),
     jancode=VALUES(jancode),
     mapcode=VALUES(mapcode),
     maker_name_kana=VALUES(maker_name_kana),
     salesprice=VALUES(salesprice),
     specialprice=VALUES(specialprice),
     selltypeid=VALUES(selltypeid),
     conditionid=VALUES(conditionid),
     sellstatusid=VALUES(sellstatusid),
     pricedownflag=VALUES(pricedownflag),
     recommendflag=VALUES(recommendflag),
     econlyflag=VALUES(econlyflag),
     newstockflag=VALUES(newstockflag),
     limitedflag=VALUES(limitedflag),
     newproductflag=VALUES(newproductflag),
     raremodelflag=VALUES(raremodelflag),
     beginnerflag=VALUES(beginnerflag),
     businessflag=VALUES(businessflag),
     reviewcount=VALUES(reviewcount),
     reviewrating=VALUES(reviewrating),
     point=VALUES(point),
     subtitle=VALUES(subtitle),
     usednum=VALUES(usednum),
     usedsalespricemin=VALUES(usedsalespricemin),
     usedsalespointmin=VALUES(usedsalespointmin),
     accessories=VALUES(accessories),
     category_name=VALUES(category_name),
     bestbadgeflag=VALUES(bestbadgeflag),
     usedconditionrank=VALUES(usedconditionrank),
     logisticstockdispkbn=VALUES(logisticstockdispkbn),
     videoflag=VALUES(videoflag),
     updatetime=VALUES(updatetime)
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
