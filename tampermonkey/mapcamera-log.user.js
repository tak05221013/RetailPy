// ==UserScript==
// @name         MapCamera ItemSearch Request+Response Logger + Auto Reload
// @namespace    https://www.mapcamera.com/
// @version      1.5.1
// @description  Log request/response for MapCamera itemsearch API calls and auto-reload after first response
// @match        https://www.mapcamera.com/*
// @run-at       document-start
// @grant GM_xmlhttpRequest
// @connect 160.251.10.136
// ==/UserScript==

(() => {
  "use strict";

  // ---- Target endpoint ----
  const ITEMSEARCH_PATH = "/ec/api/itemsearch";
  const ITEMSEARCH_ABS_PREFIX = "https://www.mapcamera.com" + ITEMSEARCH_PATH;

  // ---- Log controls ----
  const MAX_LOG_CHARS = 20_000;

  // ---- Docs ingest ----
  const DOCS_INGEST_ENABLED = true;
  const DOCS_INGEST_URL = "http://160.251.10.136:8000/mapcamera-search-docs";
  const DOCS_INGEST_API_KEY = "golden";
  const DOCS_INGEST_GENPIN_KEY = "__mc_genpin_ids_v1";
  // ---- Auto reload controls ----
  const ENABLE_AUTO_RELOAD = true;

  // 「最初のレスポンスをログしたら」何秒後にリロードするか
  const RELOAD_DELAY_MS = 1000;

  // リロードの最短間隔（短すぎると負荷＆制限の原因になりやすい）
  const MIN_RELOAD_INTERVAL_MS = 2000;

  // 同一タブで最大何回までリロードするか（無限ループ防止）
  const MAX_RELOADS_PER_TAB = 10000;

  // タブが裏にあるときはリロードしない（任意）
  const RELOAD_ONLY_WHEN_VISIBLE = false;

  // ---- Internal state (per-tab) ----
  const STATE_KEY = "__mc_auto_reload_state_v1";
  const now = () => Date.now();

  const loadState = () => {
    try {
      const raw = sessionStorage.getItem(STATE_KEY);
      if (!raw) return { reloads: 0, lastReloadAt: 0 };
      const s = JSON.parse(raw);
      return {
        reloads: Number.isFinite(s.reloads) ? s.reloads : 0,
        lastReloadAt: Number.isFinite(s.lastReloadAt) ? s.lastReloadAt : 0,
      };
    } catch {
      return { reloads: 0, lastReloadAt: 0 };
    }
  };

  const saveState = (s) => {
    try {
      sessionStorage.setItem(STATE_KEY, JSON.stringify(s));
    } catch {
      // ignore
    }
  };

  const loadPostedGenpinIds = () => {
    try {
      const raw = sessionStorage.getItem(DOCS_INGEST_GENPIN_KEY);
      if (!raw) return new Set();
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return new Set();
      return new Set(parsed.map((value) => String(value)));
    } catch {
      return new Set();
    }
  };

  const savePostedGenpinIds = (set) => {
    try {
      sessionStorage.setItem(DOCS_INGEST_GENPIN_KEY, JSON.stringify(Array.from(set)));
    } catch {
      // ignore
    }
  };

  const extractGenpinId = (doc) => {
    if (!doc || typeof doc !== "object") return null;
    if (doc.genpinId != null) return String(doc.genpinId);
    if (doc.genpin_id != null) return String(doc.genpin_id);
    return null;
  };

  const truncate = (s) => {
    if (typeof s !== "string") return s;
    if (s.length <= MAX_LOG_CHARS) return s;
    return s.slice(0, MAX_LOG_CHARS) + ` ...[truncated ${s.length - MAX_LOG_CHARS} chars]`;
  };

  const safeJsonParse = (text) => {
    try {
      return JSON.parse(text);
    } catch {
      return null;
    }
  };

  const toAbsoluteUrl = (url) => {
    try {
      if (typeof url !== "string") return null;
      return new URL(url, location.origin).href;
    } catch {
      return null;
    }
  };

  const isItemSearchUrl = (url) => {
    const abs = toAbsoluteUrl(url);
    if (!abs) return false;
    return abs.startsWith(ITEMSEARCH_ABS_PREFIX);
  };

  const normalizePayload = (body) => {
    if (body == null) return null;

    if (typeof body === "string") {
      const parsed = safeJsonParse(body);
      return parsed ?? truncate(body);
    }

    if (body instanceof URLSearchParams) {
      return truncate(body.toString());
    }

    if (body instanceof FormData) {
      const data = {};
      for (const [key, value] of body.entries()) {
        if (Object.prototype.hasOwnProperty.call(data, key)) {
          const existing = data[key];
          if (Array.isArray(existing)) existing.push(value);
          else data[key] = [existing, value];
        } else {
          data[key] = value;
        }
      }
      return data;
    }

    if (body instanceof Blob) return { type: body.type, size: body.size };
    if (body instanceof ArrayBuffer) return { byteLength: body.byteLength };
    if (ArrayBuffer.isView(body)) return { byteLength: body.byteLength };

    if (typeof body === "object") return body;
    return String(body);
  };

  const logRequest = (url, payload, context, extra = {}) => {
    if (!isItemSearchUrl(url)) return;
    const abs = toAbsoluteUrl(url);
    console.log("[MapCamera][itemsearch][request]", context, abs, normalizePayload(payload), extra);
  };

  const logResponse = (url, context, info) => {
    if (!isItemSearchUrl(url)) return;
    const abs = toAbsoluteUrl(url);
    if (Array.isArray(info?.docs)) {
      const { docs, ...meta } = info;
      console.log("[MapCamera][itemsearch][response]", context, abs, {
        ...meta,
        docsCount: docs.length,
      });
      return;
    }
    console.log("[MapCamera][itemsearch][response]", context, abs, info);
  };

  const pickDocs = (payload) => {
    if (!payload || typeof payload !== "object") return null;
    const response = payload.response;
    if (!response || typeof response !== "object") return null;
    const docs = response.docs;
    return Array.isArray(docs) ? docs : null;
  };

  const buildResponseInfo = (payload, meta) => {
    const docs = pickDocs(payload);
    if (docs) return { ...meta, docs };
    return { ...meta, body: payload };
  };

  const postDocs = async (docs, context) => {
    if (!DOCS_INGEST_ENABLED) return;
    if (!DOCS_INGEST_API_KEY) {
      console.warn("[MapCamera][docs][skip] missing DOCS_INGEST_API_KEY");
      return;
    }
    if (!Array.isArray(docs) || docs.length === 0) return;
    const postedGenpinIds = loadPostedGenpinIds();
    const docsToPost = docs.filter((doc) => {
      const genpinId = extractGenpinId(doc);
      if (!genpinId) return true;
      return !postedGenpinIds.has(genpinId);
    });
    if (docsToPost.length === 0) return;
    try {
      await new Promise((resolve, reject) => {
        GM_xmlhttpRequest({
          method: "POST",
          url: DOCS_INGEST_URL,
          headers: {
            "content-type": "application/json",
            "x-api-key": DOCS_INGEST_API_KEY,
          },
          data: JSON.stringify({
            client_ts_ms: Date.now(),
            page_url: location.href,
            context,
            docs: docsToPost,
          }),
          onload: () => resolve(),
          onerror: (err) => reject(err),
          ontimeout: () => reject(new Error("timeout")),
        });
      });
      console.log("[MapCamera][docs][posted]", {
        context,
        count: docsToPost.length,
      });
      docsToPost.forEach((doc) => {
        const genpinId = extractGenpinId(doc);
        if (genpinId) postedGenpinIds.add(genpinId);
      });
      savePostedGenpinIds(postedGenpinIds);
    } catch (e) {
      console.warn("[MapCamera][docs][error]", String(e));
    }
  };
  const handleDocs = async (docs, context) => {
    if (!docs || docs.length === 0) return;
    await postDocs(docs, context);
  };

  // ---- Auto reload trigger: "first response in this page load" ----
  let reloadScheduledThisPage = false;

  const scheduleReloadOnce = (reason) => {
    if (!ENABLE_AUTO_RELOAD) return;
    if (reloadScheduledThisPage) return;

    if (RELOAD_ONLY_WHEN_VISIBLE && document.visibilityState !== "visible") {
      console.log("[MapCamera][auto-reload] skipped (tab not visible)", { reason });
      return;
    }

    const state = loadState();

    if (state.reloads >= MAX_RELOADS_PER_TAB) {
      console.warn("[MapCamera][auto-reload] reached MAX_RELOADS_PER_TAB; stopping", state);
      return;
    }

    const elapsed = now() - state.lastReloadAt;
    if (state.lastReloadAt && elapsed < MIN_RELOAD_INTERVAL_MS) {
      console.log("[MapCamera][auto-reload] skipped (too soon)", {
        reason,
        elapsedMs: elapsed,
        minIntervalMs: MIN_RELOAD_INTERVAL_MS,
      });
      return;
    }

    reloadScheduledThisPage = true;

    console.log("[MapCamera][auto-reload] scheduled", { reason, inMs: RELOAD_DELAY_MS });

    setTimeout(() => {
      const s2 = loadState();
      s2.reloads += 1;
      s2.lastReloadAt = now();
      saveState(s2);

      console.log("[MapCamera][auto-reload] reloading now", s2);
      location.reload();
    }, RELOAD_DELAY_MS);
  };

  // -------------------------
  // fetch: request + response
  // -------------------------
  const originalFetch = window.fetch.bind(window);

  window.fetch = async (input, init = {}) => {
    const urlRaw = typeof input === "string" ? input : input?.url;
    const url = toAbsoluteUrl(urlRaw) ?? urlRaw;

    const method =
        init?.method ||
        (typeof input !== "string" ? input?.method : undefined) ||
        "GET";

    logRequest(url, init?.body, "fetch", { method });

    const res = await originalFetch(input, init);

    try {
      if (isItemSearchUrl(url)) {
        const ct = res.headers?.get?.("content-type") || "";
        const clone = res.clone();

        if (ct.includes("application/json")) {
          const data = await clone.json();
          logResponse(url, "fetch", buildResponseInfo(data, { status: res.status, contentType: ct }));
          const docs = pickDocs(data);
          if (docs) {
            void handleDocs(docs, "fetch");
          }
        } else {
          const text = await clone.text();
          const parsed = safeJsonParse(text);
          logResponse(
            url,
            "fetch",
            buildResponseInfo(parsed ?? truncate(text), { status: res.status, contentType: ct }),
          );
          const docs = pickDocs(parsed);
          if (docs) {
            void handleDocs(docs, "fetch");
          }
        }

        // ★「レスポンスを1回ログした」＝処理完了、でリロード
        scheduleReloadOnce("fetch response logged");
      }
    } catch (e) {
      if (isItemSearchUrl(url)) {
        logResponse(url, "fetch", { status: res.status, error: String(e) });
        scheduleReloadOnce("fetch response error logged");
      }
    }

    return res;
  };

  // -------------------------
  // XHR: request + response
  // -------------------------
  const originalOpen = XMLHttpRequest.prototype.open;
  const originalSend = XMLHttpRequest.prototype.send;
  const originalSetRequestHeader = XMLHttpRequest.prototype.setRequestHeader;

  XMLHttpRequest.prototype.setRequestHeader = function setRequestHeader(name, value) {
    try {
      if (!this.__mcHeaders) this.__mcHeaders = [];
      this.__mcHeaders.push([String(name), String(value)]);
    } catch {
      // ignore
    }
    return originalSetRequestHeader.call(this, name, value);
  };

  XMLHttpRequest.prototype.open = function open(method, url, ...rest) {
    this.__mcUrlRaw = url;
    this.__mcUrlAbs = toAbsoluteUrl(url) ?? url;
    this.__mcMethod = method;

    if (!this.__mcHooked) {
      this.__mcHooked = true;

      this.addEventListener("loadend", function () {
        const u = this.__mcUrlAbs ?? this.__mcUrlRaw;
        if (!isItemSearchUrl(u)) return;

        const status = this.status;
        const ct = (this.getResponseHeader && this.getResponseHeader("content-type")) || "";

        try {
          if (this.responseType === "" || this.responseType === "text") {
            const text = this.responseText ?? "";
            const parsed = safeJsonParse(text);
            logResponse(
              u,
              "xhr",
              buildResponseInfo(parsed ?? truncate(text), {
                status,
                contentType: ct,
                responseType: this.responseType || "text",
              }),
            );
            const docs = pickDocs(parsed);
            if (docs) {
              void handleDocs(docs, "xhr");
            }
          } else if (this.responseType === "json") {
            logResponse(
              u,
              "xhr",
              buildResponseInfo(this.response, { status, contentType: ct, responseType: "json" }),
            );
            const docs = pickDocs(this.response);
            if (docs) {
              void handleDocs(docs, "xhr");
            }
          } else if (this.responseType === "arraybuffer") {
            const ab = this.response;
            logResponse(u, "xhr", {
              status,
              contentType: ct,
              responseType: "arraybuffer",
              body: ab ? { byteLength: ab.byteLength } : null,
            });
          } else if (this.responseType === "blob") {
            const b = this.response;
            logResponse(u, "xhr", {
              status,
              contentType: ct,
              responseType: "blob",
              body: b ? { type: b.type, size: b.size } : null,
            });
          } else {
            logResponse(u, "xhr", {
              status,
              contentType: ct,
              responseType: this.responseType,
              body: "[unlogged responseType]",
            });
          }

          // ★「レスポンスを1回ログした」＝処理完了、でリロード
          scheduleReloadOnce("xhr response logged");
        } catch (e) {
          logResponse(u, "xhr", { status, contentType: ct, error: String(e) });
          scheduleReloadOnce("xhr response error logged");
        }
      });
    }

    return originalOpen.call(this, method, url, ...rest);
  };

  XMLHttpRequest.prototype.send = function send(body) {
    const u = this.__mcUrlAbs ?? this.__mcUrlRaw;
    logRequest(u, body, "xhr", {
      method: this.__mcMethod || "GET",
      headersSetByJs: this.__mcHeaders || [],
    });
    return originalSend.call(this, body);
  };

  // 起動ログ
  console.log("[MapCamera] logger+auto-reload loaded", {
    ENABLE_AUTO_RELOAD,
    RELOAD_DELAY_MS,
    MIN_RELOAD_INTERVAL_MS,
    MAX_RELOADS_PER_TAB,
  });
})();
