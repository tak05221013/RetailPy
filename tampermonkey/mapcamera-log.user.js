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
  const DOCS_INGEST_DETAIL_CONCURRENCY = 4;
  const DOCS_INGEST_DETAIL_URL = "http://160.251.10.136:8000/mapcamera-doc-detail";
  const DOCS_INGEST_DETAIL_SELECTOR = ".infobox.clearfix";
  const DOCS_INGEST_DETAIL_TIMEOUT_MS = 8000;
  const JANCODE_MST_URL = "http://160.251.10.136:8000/mapcamera-jancode-mst";
  const JANCODE_MST_STORAGE_KEY = "__mc_jancode_mst_v1";

  // ---- Auto reload controls ----
  const ENABLE_AUTO_RELOAD = true;

  // 「最初のレスポンスをログしたら」何秒後にリロードするか
  const RELOAD_DELAY_MS = 30000;

  // リロードの最短間隔（短すぎると負荷＆制限の原因になりやすい）
  const MIN_RELOAD_INTERVAL_MS = 30000;

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

  const loadJancodeMst = () => {
    try {
      const raw = sessionStorage.getItem(JANCODE_MST_STORAGE_KEY);
      if (!raw) return null;
      return JSON.parse(raw);
    } catch {
      return null;
    }
  };

  const saveJancodeMst = (data) => {
    try {
      sessionStorage.setItem(JANCODE_MST_STORAGE_KEY, JSON.stringify(data));
    } catch {
      // ignore
    }
  };

  const getJancodeMasterPrice = (mst, janCode) => {
    if (!mst || !janCode) return null;
    const prices = mst?.prices ?? mst?.price ?? mst;
    if (!prices || typeof prices !== "object") return null;
    const raw = prices[janCode];
    return numberOrNull(raw);
  };

  const extractGenpinId = (doc) => {
    if (!doc || typeof doc !== "object") return null;
    if (doc.genpinId != null) return String(doc.genpinId);
    if (doc.genpin_id != null) return String(doc.genpin_id);
    return null;
  };

  const extractJanCode = (doc) => {
    if (!doc || typeof doc !== "object") return null;
    const raw = doc.jancode ?? doc.janCode ?? doc.jan_code ?? doc.jan;
    if (raw == null) return null;
    const value = String(raw).trim();
    return value ? value : null;
  };

  const extractCond = (doc) => {
    if (!doc || typeof doc !== "object") return null;
    const raw = doc.conditionid ?? doc.conditionId ?? doc.cond ?? doc.condition;
    const cond = Number(raw);
    return Number.isFinite(cond) ? cond : null;
  };

  const numberOrNull = (value) => {
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
  };

  const calculateDocPrice = (doc) => {
    const specialprice = numberOrNull(doc?.specialprice) ?? 0;
    const salesprice = numberOrNull(doc?.salesprice) ?? 0;
    if (specialprice !== 0) {
      return Math.trunc(specialprice + specialprice * 0.1);
    }
    if (salesprice === 0) return null;
    return Math.trunc(salesprice + salesprice * 0.1);
  };

  const buildDetailTimestamp = () => {
    const nowTs = Date.now();
    const nowDate = new Date(nowTs);
    const date = nowDate.toISOString().slice(0, 10);
    const time = nowDate.toTimeString().slice(0, 8);
    return { unixtime: nowTs, date, time };
  };

  const truncate = (s) => {
    if (typeof s !== "string") return s;
    if (s.length <= MAX_LOG_CHARS) return s;
    return s.slice(0, MAX_LOG_CHARS) + ` ...[truncated ${s.length - MAX_LOG_CHARS} chars]`;
  };

  const normalizeWhitespace = (value) => {
    if (typeof value !== "string") return "";
    return value.replace(/\s+/g, " ").trim();
  };

  const pickHeadingSections = (root, headingText) => {
    if (!root) return [];
    const headings = Array.from(root.querySelectorAll("h3"));
    return headings
      .filter((h3) => normalizeWhitespace(h3.textContent).includes(headingText))
      .map((h3) => h3.parentElement || h3)
      .filter(Boolean);
  };

  const dedupeTexts = (items) => {
    const seen = new Set();
    return items.filter((item) => {
      if (!item) return false;
      if (seen.has(item)) return false;
      seen.add(item);
      return true;
    });
  };

  const extractAccessoriesTexts = (root) => {
    const sections = pickHeadingSections(root, "付属品");
    if (!sections.length) return [];
    const values = sections
      .map((section) => {
        const paragraph = section.querySelector("p");
        const raw = paragraph ? paragraph.textContent : section.textContent;
        return normalizeWhitespace(raw).replace(/^付属品\s*/u, "");
      })
      .filter((value) => value);
    return dedupeTexts(values);
  };

  const extractStaffComments = (root) => {
    if (!root) return [];
    const markers = Array.from(root.querySelectorAll("b")).filter((b) =>
      normalizeWhitespace(b.textContent).includes("点検スタッフからのコメント"),
    );
    if (!markers.length) return [];
    const values = markers
      .map((marker) => {
        const parent = marker.parentElement || marker;
        const raw = normalizeWhitespace(parent.textContent);
        return raw.replace(/◎?点検スタッフからのコメント\s*/u, "").trim();
      })
      .filter((value) => value);
    return dedupeTexts(values);
  };

  const extractDetailKeywordSections = (detailRoot) => {
    if (!detailRoot) return [];
    const keywords = ["付属品", "点検スタッフからのコメント"];
    const values = Array.from(detailRoot.querySelectorAll("*"))
      .map((el) => normalizeWhitespace(el.textContent))
      .filter(
        (text) => text && keywords.some((keyword) => text.includes(keyword)),
      );
    return dedupeTexts(values);
  };

  const extractConditionText = (root) => {
    if (!root) return null;
    const row = root.querySelector(".conditionbox .conditionlist tr.focus");
    if (!row) return null;
    const title = normalizeWhitespace(row.querySelector("th")?.textContent);
    const desc = normalizeWhitespace(row.querySelector("td")?.textContent);
    if (!title && !desc) return null;
    if (title && desc) return `${title} - ${desc}`;
    return title || desc;
  };

  const buildDetailDescription = (root) => {
    if (!root) return "";
    const sections = [];
    const keywordSections = extractDetailKeywordSections(root);
    if (keywordSections.length > 0) {
      keywordSections.forEach((item) => {
        sections.push(item);
      });
    } else {
      const accessories = extractAccessoriesTexts(root);
      accessories.forEach((item) => {
        sections.push(`付属品: ${item}`);
      });
      const comments = extractStaffComments(root);
      comments.forEach((item) => {
        sections.push(`点検スタッフからのコメント: ${item}`);
      });
    }
    const condition = extractConditionText(root);
    if (condition) sections.push(`商品コンディション: ${condition}`);
    if (sections.length > 0) {
      return sections.join("\n");
    }
    return normalizeWhitespace(root.textContent);
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

  const DOC_URL_FIELDS = [
    "url",
    "item_url",
    "itemUrl",
    "detail_url",
    "detailUrl",
    "link",
    "href",
    "itemLink",
    "page_url",
    "pageUrl",
  ];

  const DOC_MAPCODE_FIELDS = [
    "mapcode",
    "map_code",
    "mapCode",
  ];

  const buildItemUrlFromMapcode = (mapcode) => {
    if (typeof mapcode !== "string") return null;
    const trimmed = mapcode.trim();
    if (!trimmed) return null;
    return `https://www.mapcamera.com/item/${trimmed}`;
  };

  const extractDocMapcode = (doc) => {
    if (!doc || typeof doc !== "object") return null;
    for (const field of DOC_MAPCODE_FIELDS) {
      const value = doc[field];
      if (typeof value === "string" && value.trim()) return value.trim();
    }
    return null;
  };

  const extractDocUrl = (doc) => {
    if (!doc || typeof doc !== "object") return null;
    for (const field of DOC_URL_FIELDS) {
      const value = doc[field];
      if (typeof value === "string" && value.trim()) return value.trim();
    }
    const mapcode = extractDocMapcode(doc);
    if (mapcode) return buildItemUrlFromMapcode(mapcode);
    return null;
  };

  const buildCond7Url = (url) => {
    const abs = toAbsoluteUrl(url);
    if (!abs) return null;
    const u = new URL(abs);
    u.searchParams.set("cond", "7");
    return u.href;
  };

  const runWithConcurrency = async (items, limit, handler) => {
    const total = Array.isArray(items) ? items.length : 0;
    if (!total) return;
    const concurrency = Math.max(1, Number(limit) || 1);
    let cursor = 0;
    const workers = Array.from({ length: Math.min(concurrency, total) }, async () => {
      while (cursor < total) {
        const index = cursor++;
        await handler(items[index], index);
      }
    });
    await Promise.all(workers);
  };

  const fetchDocDetailInfo = async (doc, index, context) => {
    const rawUrl = extractDocUrl(doc);
    if (!rawUrl) {
      console.warn("[MapCamera][docs][detail][skip] missing url", { context, index });
      return null;
    }
    const condUrl = buildCond7Url(rawUrl);
    if (!condUrl) {
      console.warn("[MapCamera][docs][detail][skip] invalid url", { context, index, rawUrl });
      return null;
    }
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), DOCS_INGEST_DETAIL_TIMEOUT_MS);
    try {
      const res = await originalFetch(condUrl, {
        credentials: "include",
        cache: "no-store",
        signal: controller.signal,
      });
      const html = await res.text();
      const docDom = new DOMParser().parseFromString(html, "text/html");
      const detailRoot = docDom.querySelector(DOCS_INGEST_DETAIL_SELECTOR);
      const infoText = buildDetailDescription(detailRoot);
      return {
        context,
        index,
        url: condUrl,
        status: res.status,
        selector: DOCS_INGEST_DETAIL_SELECTOR,
        text: infoText,
      };
    } catch (e) {
      const isAbort = e instanceof DOMException && e.name === "AbortError";
      console.warn(
        isAbort ? "[MapCamera][docs][detail][timeout]" : "[MapCamera][docs][detail][error]",
        {
          context,
          index,
          url: condUrl,
          error: String(e),
          timeoutMs: isAbort ? DOCS_INGEST_DETAIL_TIMEOUT_MS : undefined,
        },
      );
      return null;
    } finally {
      clearTimeout(timeoutId);
    }
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
    docsToPost.forEach((doc) => {
      const genpinId = extractGenpinId(doc);
      if (genpinId) postedGenpinIds.add(genpinId);
    });
    savePostedGenpinIds(postedGenpinIds);
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
    } catch (e) {
      console.warn("[MapCamera][docs][error]", String(e));
    }
  };

  const postDocDetail = async (detail) => {
    if (!DOCS_INGEST_ENABLED) return false;
    if (!DOCS_INGEST_API_KEY) {
      console.warn("[MapCamera][detail][skip] missing DOCS_INGEST_API_KEY");
      return false;
    }
    try {
      await new Promise((resolve, reject) => {
        GM_xmlhttpRequest({
          method: "POST",
          url: DOCS_INGEST_DETAIL_URL,
          headers: {
            "content-type": "application/json",
            "x-api-key": DOCS_INGEST_API_KEY,
          },
          data: JSON.stringify(detail),
          onload: (response) => {
            if (response.status < 200 || response.status >= 300) {
              reject(new Error(`status ${response.status}`));
              return;
            }
            resolve();
          },
          onerror: (err) => reject(err),
          ontimeout: () => reject(new Error("timeout")),
        });
      });
      return true;
    } catch (e) {
      console.warn("[MapCamera][detail][error]", String(e));
      return false;
    }
  };

  const postCond7DocsWithDetail = async (docs, context) => {
    await runWithConcurrency(docs, DOCS_INGEST_DETAIL_CONCURRENCY, async (doc, index) => {
      const detailInfo = await fetchDocDetailInfo(doc, index, context);
      if (!detailInfo) return;
      const janCode = extractJanCode(doc);
      const genpinId = extractGenpinId(doc);
      const cond = extractCond(doc);
      const price = calculateDocPrice(doc);
      if (!janCode || !genpinId || price == null || cond == null) {
        console.warn("[MapCamera][detail][skip] missing required fields", {
          context,
          index,
          janCode,
          genpinId,
          cond,
          price,
        });
        return;
      }
      const timestamp = buildDetailTimestamp();
      const detailPayload = {
        jan: janCode,
        genpinId,
        price,
        cond,
        dsc: detailInfo.text,
        ...timestamp,
      };
      const detailPosted = await postDocDetail(detailPayload);
      if (detailPosted) {
        await postDocs([doc], context);
      }
    });
  };

  const downloadJancodeMst = async () => {
    if (!DOCS_INGEST_API_KEY) {
      console.warn("[MapCamera][jancode][skip] missing DOCS_INGEST_API_KEY");
      return null;
    }
    try {
      const data = await new Promise((resolve, reject) => {
        GM_xmlhttpRequest({
          method: "GET",
          url: JANCODE_MST_URL,
          headers: {
            "x-api-key": DOCS_INGEST_API_KEY,
          },
          onload: (response) => {
            if (response.status < 200 || response.status >= 300) {
              reject(new Error(`status ${response.status}`));
              return;
            }
            const parsed = safeJsonParse(response.responseText);
            if (!parsed) {
              reject(new Error("invalid json"));
              return;
            }
            resolve(parsed);
          },
          onerror: (err) => reject(err),
          ontimeout: () => reject(new Error("timeout")),
        });
      });
      saveJancodeMst(data);
      console.log("[MapCamera][jancode][downloaded]", {
        updated_at: data?.updated_at,
        pricesCount: data?.prices ? Object.keys(data.prices).length : 0,
      });
      return data;
    } catch (e) {
      console.warn("[MapCamera][jancode][error]", String(e));
      return null;
    }
  };

  const ensureJancodeMst = () => {
    const existing = loadJancodeMst();
    if (existing) return;
    void downloadJancodeMst();
  };

  const getJancodeMst = async () => {
    const existing = loadJancodeMst();
    if (existing) return existing;
    return await downloadJancodeMst();
  };

  const filterDocsByPrice = (docs, jancodeMst) => {
    const cond7Docs = [];
    const otherDocs = [];
    docs.forEach((doc) => {
      const janCode = extractJanCode(doc);
      if (!janCode) return;
      const masterPrice = getJancodeMasterPrice(jancodeMst, janCode);
      if (masterPrice == null) return;
      const price = calculateDocPrice(doc);
      if (price == null) return;
      const margin = masterPrice - (masterPrice * 0.1 + 1434 + price);
      if (margin < 3000) return;
      const cond = extractCond(doc);
      if (cond === 7) {
        cond7Docs.push(doc);
      } else {
        otherDocs.push(doc);
      }
    });
    return { cond7Docs, otherDocs };
  };

  const handleDocs = async (docs, context) => {
    if (!docs || docs.length === 0) return;
    const jancodeMst = await getJancodeMst();
    if (!jancodeMst) {
      console.warn("[MapCamera][docs][skip] missing jancode mst");
      return;
    }
    const { cond7Docs, otherDocs } = filterDocsByPrice(docs, jancodeMst);
    if (otherDocs.length > 0) {
      await postDocs(otherDocs, context);
    }
    if (cond7Docs.length > 0) {
      await postCond7DocsWithDetail(cond7Docs, context);
    }
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
  ensureJancodeMst();
})();
