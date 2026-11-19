/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Version selector and "not the latest version" banner for the multi-version docs site.
 *
 * Layout: shared pages at /, one directory per version (/0.3/, /main/), /latest/
 * redirect stubs, and /versions.json (schema in scripts/docs_versions.py).
 */
(function () {
  "use strict";

  // Keep in sync with scripts/docs_versions.py.
  const VERSION_SEGMENT = /^(?:main|latest|[0-9]+\.[0-9]+)$/;
  const LATEST = "latest";
  const SWITCHER_CLASS = "docs-version-switcher";
  const BANNER_ID = "docs-version-banner";
  // Namespaced because GitHub Pages projects share one origin.
  const VERSION_STORAGE_PREFIX = "superset-kubernetes-operator-docs-version:";

  // ---- paths ----

  function pathParts(pathname) {
    return pathname.split("/").filter(Boolean);
  }

  function toDirPath(parts) {
    return parts.length ? `/${parts.join("/")}/` : "/";
  }

  /** Everything before the version segment. Not assumed to be "/": the site may live under a subpath. */
  function resolveSiteRoot() {
    const parts = pathParts(window.location.pathname);
    const index = parts.findIndex((part) => VERSION_SEGMENT.test(part));
    if (index !== -1) {
      return toDirPath(parts.slice(0, index));
    }
    // Shared pages have no version segment; use the theme's scope instead.
    const scope = window.__md_scope;
    return scope instanceof URL ? toDirPath(pathParts(scope.pathname)) : "/";
  }

  function detectVersionSegment(root) {
    const first = pathParts(window.location.pathname.slice(root.length))[0];
    return first && VERSION_SEGMENT.test(first) ? first : null;
  }

  /** Same page in another version. Path only; callers add search/hash. */
  function buildVersionPath(root, targetSegment, currentSegment) {
    const parts = pathParts(window.location.pathname.slice(root.length));
    if (currentSegment && parts[0] === currentSegment) {
      parts.shift();
    }
    const trailingSlash = parts.length > 0 && window.location.pathname.endsWith("/");
    return `${root}${targetSegment}/${parts.join("/")}${trailingSlash ? "/" : ""}`;
  }

  // ---- versions ----

  const isLatest = (entry) => entry.aliases.includes(LATEST);
  const segmentFor = (entry) => (isLatest(entry) ? LATEST : entry.version);

  function findEntry(versions, segment) {
    if (!segment) return undefined;
    if (segment === LATEST) return versions.find(isLatest);
    return versions.find((entry) => entry.version === segment);
  }

  // Shared pages belong to no version, so without a remembered choice a reader on
  // 0.1 who visits one and goes back would land on /latest/. Per tab on purpose.
  function getPreferredVersion(root, versions) {
    try {
      const preferred = window.sessionStorage.getItem(`${VERSION_STORAGE_PREFIX}${root}`);
      return versions.find((entry) => entry.version === preferred);
    } catch (error) {
      console.warn("version-switcher: could not read the saved documentation version", error);
      return undefined;
    }
  }

  function savePreferredVersion(root, version) {
    try {
      window.sessionStorage.setItem(`${VERSION_STORAGE_PREFIX}${root}`, version);
    } catch (error) {
      console.warn("version-switcher: could not save the selected documentation version", error);
    }
  }

  /** Assigning link.pathname rewrites href as an absolute URL; set the attribute instead. */
  function setLinkPath(link, pathname) {
    link.setAttribute("href", `${pathname}${link.search}${link.hash}`);
  }

  /** Return the selected-version path for a versioned link, or null when unrelated. */
  function retargetedPath(root, version, link, { aliasOnly = false } = {}) {
    if (link.origin !== window.location.origin || !link.pathname.startsWith(root)) return null;
    const path = link.pathname.slice(root.length);
    const first = pathParts(path)[0];
    if (!first || !VERSION_SEGMENT.test(first) || (aliasOnly && first !== LATEST)) return null;
    return `${root}${version}/${path.slice(first.length + 1)}`;
  }

  // Shared pages link to /latest/...; point them at the reader's version instead.
  function rewriteRootVersionLinks(root, version) {
    document.querySelectorAll("a[href]").forEach((link) => {
      if (link.hasAttribute("data-version-link") || link.closest(`.${SWITCHER_CLASS}`)) return;
      if (!link.dataset.latestPath) {
        if (retargetedPath(root, version, link, { aliasOnly: true }) === null) return;
        link.dataset.latestPath = link.pathname;
      }
      const pathname = retargetedPath(root, version, {
        origin: link.origin,
        pathname: link.dataset.latestPath,
      });
      if (pathname !== null) setLinkPath(link, pathname);
    });
  }

  function normalizeVersions(raw) {
    if (!Array.isArray(raw)) return [];
    return raw
      .filter((entry) => entry && typeof entry.version === "string" && VERSION_SEGMENT.test(entry.version))
      .map((entry) => ({
        version: entry.version,
        title: entry.title || entry.version,
        aliases: Array.isArray(entry.aliases) ? entry.aliases : [],
      }));
  }

  /**
   * Only local previews may use a fallback. Published pages hide the switcher
   * when the shared manifest is missing or invalid.
   */
  function loadVersions(root) {
    if (!root) return Promise.resolve([]);
    return fetch(`${root}versions.json`)
      .then((response) => {
        if (!response.ok) throw new Error(`versions.json returned ${response.status}`);
        return response.json();
      })
      .then((raw) => {
        const versions = normalizeVersions(raw);
        if (versions.length) return versions;
        throw new Error("versions.json contains no valid versions");
      })
      .catch((error) => {
        console.warn("version-switcher: could not load versions.json", error);
        return window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1"
          ? [{ version: "main", title: "main (unreleased)", aliases: [] }]
          : [];
      });
  }

  // ---- navigation ----

  // The target version may not have this page.
  function pageExists(url) {
    const signal =
      typeof AbortSignal !== "undefined" && typeof AbortSignal.timeout === "function"
        ? AbortSignal.timeout(3000)
        : undefined;
    return fetch(url, { method: "HEAD", signal })
      .then((response) => response.ok)
      .catch(() => true); // offline or blocked HEAD: just navigate
  }

  /** Keeps ?query and #hash, records the choice, and avoids a 404 when the page is missing in the target version. */
  async function onVersionLinkClick(event) {
    const link = event.target.closest("a[data-version-link]");
    const modified = event.metaKey || event.ctrlKey || event.shiftKey || event.altKey;
    if (!link || event.defaultPrevented || event.button !== 0 || modified) return;

    event.preventDefault();
    const exact = link.getAttribute("href");
    if (link.dataset.versionPreference) {
      const root = resolveSiteRoot();
      savePreferredVersion(root, link.dataset.versionPreference);
      // A shared page has no versioned twin: stay here and retarget its links.
      if (!detectVersionSegment(root)) {
        rewriteRootVersionLinks(root, link.dataset.versionPreference);
        render(await versionsPromise);
        return;
      }
    }
    const destination = (await pageExists(exact))
      ? exact + window.location.search + window.location.hash
      : link.dataset.fallback;
    window.location.assign(destination);
  }

  function makeVersionLink(root, entryOrSegment, currentSegment) {
    const segment = typeof entryOrSegment === "string" ? entryOrSegment : segmentFor(entryOrSegment);
    const link = document.createElement("a");
    // Shared pages have no counterpart in other versions, so link to the version's landing page.
    link.href = currentSegment
      ? buildVersionPath(root, segment, currentSegment)
      : `${root}${segment}/`;
    link.dataset.versionLink = "";
    link.dataset.fallback = `${root}${segment}/`;
    if (typeof entryOrSegment !== "string") {
      link.dataset.versionPreference = entryOrSegment.version;
    }
    return link;
  }

  // ---- selector ----

  function createSelector() {
    const nav = document.createElement("nav");
    nav.className = `md-version ${SWITCHER_CLASS}`;
    nav.setAttribute("aria-label", "Documentation version");

    const toggle = document.createElement("button");
    toggle.type = "button";
    toggle.className = "md-version__current";
    toggle.setAttribute("aria-label", "Select documentation version");
    toggle.setAttribute("aria-expanded", "false");
    const label = document.createElement("span");
    label.className = `${SWITCHER_CLASS}__label`;
    toggle.appendChild(label);

    const list = document.createElement("ul");
    list.className = "md-version__list";
    toggle.setAttribute("aria-controls", `${SWITCHER_CLASS}-list`);
    list.id = `${SWITCHER_CLASS}-list`;
    toggle.addEventListener("click", () => {
      const expanded = toggle.getAttribute("aria-expanded") === "true";
      toggle.setAttribute("aria-expanded", String(!expanded));
      nav.classList.toggle("md-version--active", !expanded);
    });
    list.addEventListener("click", onVersionLinkClick);

    nav.append(toggle, list);
    const header = document.querySelector(".md-header__inner");
    const title = header?.querySelector('[data-md-component="header-title"]');
    if (header) {
      header.insertBefore(nav, title?.nextSibling || null);
    }
    return nav;
  }

  function renderSelector(ctx) {
    const { versions, root, segment, current, latest, preferred } = ctx;
    if (!document.querySelector(".md-header__inner")) return;
    const nav = document.querySelector(`.${SWITCHER_CLASS}`) || createSelector();
    // On a shared page, show the last-viewed version rather than latest.
    const selected = current || (segment ? { title: segment } : preferred || latest || versions[0]);

    nav.querySelector(`.${SWITCHER_CLASS}__label`).textContent = selected.title;

    const items = versions.map((entry) => {
      const link = makeVersionLink(root, entry, segment);
      link.className = "md-version__link";
      link.textContent = isLatest(entry) ? `${entry.title} (latest)` : entry.title;
      if (entry === selected) link.setAttribute("aria-current", "page");
      const item = document.createElement("li");
      item.className = "md-version__item";
      item.appendChild(link);
      return item;
    });
    nav.querySelector(".md-version__list").replaceChildren(...items);
  }

  // ---- banner ----

  // Rendered at runtime: a published 0.3 can't know 0.4 exists, but versions.json can.
  function renderBanner(ctx) {
    const { root, segment, current, latest } = ctx;
    document.getElementById(BANNER_ID)?.remove();
    if (!current || !latest || current === latest) return;

    const message =
      current.version === "main"
        ? "This is the development version of the documentation and may describe unreleased features. "
        : `You are viewing the documentation for version ${current.title}, which is not the latest release. `;

    const link = makeVersionLink(root, latest, segment);
    link.textContent = `Switch to the latest version (${latest.title}).`;

    const banner = document.createElement("div");
    banner.id = BANNER_ID;
    banner.className = "docs-version-banner";
    banner.setAttribute("role", "note");
    banner.append(message, link);
    banner.addEventListener("click", onVersionLinkClick);

    document.querySelector(".md-main")?.before(banner);
  }

  // ---- main ----

  // Runs on every page view, so it must be idempotent.
  function render(versions) {
    if (versions.length === 0) return;
    const root = resolveSiteRoot();
    const segment = detectVersionSegment(root);
    const latest = versions.find(isLatest);
    const preferred = getPreferredVersion(root, versions);
    if (!segment) {
      rewriteRootVersionLinks(root, preferred?.version || latest?.version || "main");
    }
    const ctx = {
      versions,
      root,
      segment,
      current: findEntry(versions, segment),
      latest,
      preferred,
    };
    if (ctx.current) {
      savePreferredVersion(root, ctx.current.version);
    }
    renderSelector(ctx);
    renderBanner(ctx);
  }

  function onPageReady(callback) {
    // Material-style instant navigation exposes document$, which emits on every page view.
    if (window.document$ && typeof window.document$.subscribe === "function") {
      window.document$.subscribe(callback);
      return;
    }
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", callback);
    } else {
      callback();
    }
  }

  const versionsPromise = loadVersions(resolveSiteRoot());
  onPageReady(() => versionsPromise.then(render));
})();
