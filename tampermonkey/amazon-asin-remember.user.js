// ==UserScript==
// @name         Amazon.co.jp ASIN Remember Linker
// @namespace    https://camiiira.com/
// @version      1.0.0
// @description  Add a link to the Amazon product title that sends the ASIN to camiiira.com
// @match        https://www.amazon.co.jp/*
// @run-at       document-idle
// @grant        none
// ==/UserScript==

(() => {
  "use strict";

  const ASIN_PATTERNS = [
    /\/dp\/([A-Z0-9]{10})(?:[/?]|$)/i,
    /\/gp\/product\/([A-Z0-9]{10})(?:[/?]|$)/i,
    /\/gp\/aw\/d\/([A-Z0-9]{10})(?:[/?]|$)/i,
    /\/product\/([A-Z0-9]{10})(?:[/?]|$)/i,
  ];

  const REMEMBER_BASE_URL = "https://camiiira.com/asin-to-remember?asin=";
  const TITLE_SELECTOR = "#productTitle";

  const extractAsinFromUrl = (url) => {
    if (!url) return null;
    for (const pattern of ASIN_PATTERNS) {
      const match = url.match(pattern);
      if (match?.[1]) return match[1].toUpperCase();
    }
    return null;
  };

  const buildRememberUrl = (asin) => `${REMEMBER_BASE_URL}${encodeURIComponent(asin)}`;

  const ensureTitleLink = () => {
    const asin = extractAsinFromUrl(location.href);
    if (!asin) return false;

    const titleEl = document.querySelector(TITLE_SELECTOR);
    if (!titleEl) return false;

    const existingLink = titleEl.closest("a");
    if (existingLink && existingLink.dataset?.asinRemember === "true") {
      return true;
    }

    const link = document.createElement("a");
    link.href = buildRememberUrl(asin);
    link.dataset.asinRemember = "true";
    link.style.cursor = "pointer";
    link.style.textDecoration = "underline";
    link.style.textDecorationColor = "currentColor";

    if (existingLink) {
      existingLink.replaceWith(link);
      link.appendChild(titleEl);
    } else {
      titleEl.replaceWith(link);
      link.appendChild(titleEl);
    }

    return true;
  };

  if (ensureTitleLink()) return;

  const observer = new MutationObserver(() => {
    if (ensureTitleLink()) observer.disconnect();
  });

  if (document.body) {
    observer.observe(document.body, { childList: true, subtree: true });
  }
})();
