/**
 * Version selector and "not the latest version" banner for the multi-version docs site.
 *
 * Layout: shared pages at /, one directory per version (/0.3/, /main/), /latest/
 * redirect stubs, and /versions.json (schema in scripts/docs_versions.py).
 */
(function () {
  "use strict";

  // Keep in sync with scripts/docs_versions.py.
  const VERSION_SEGMENT = /^(?:main|latest|\d+\.\d+)$/;
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

  // Shared pages link to /latest/...; point them at the reader's version instead.
  function rewriteRootVersionLinks(root, version) {
    document.querySelectorAll("a[href]").forEach((link) => {
      if (
        link.origin !== window.location.origin ||
        link.hasAttribute("data-version-link") ||
        link.closest(`.${SWITCHER_CLASS}`)
      ) {
        return;
      }
      const path = link.pathname.slice(root.length);
      const firstSegment = pathParts(path)[0];
      if (firstSegment && VERSION_SEGMENT.test(firstSegment)) {
        setLinkPath(link, `${root}${version}/${path.slice(firstSegment.length + 1)}`);
      }
    });
  }

  // Root-absolute links need the subpath prefix when the site is not served from "/".
  function rewriteSiteRootLinks(root) {
    if (root === "/") return;
    document.querySelectorAll("a[href]").forEach((link) => {
      if (
        link.origin !== window.location.origin ||
        link.hasAttribute("data-version-link") ||
        link.closest(`.${SWITCHER_CLASS}`)
      ) {
        return;
      }
      const path = link.pathname;
      if (!path.startsWith("/") || path.startsWith(root)) return;
      setLinkPath(link, `${root}${path.slice(1)}`);
    });
  }

  function normalizeVersions(raw) {
    if (!Array.isArray(raw)) return [];
    return raw
      .filter((entry) => entry && typeof entry.version === "string")
      .map((entry) => ({
        version: entry.version,
        title: entry.title || entry.version,
        aliases: Array.isArray(entry.aliases) ? entry.aliases : [],
      }));
  }

  /**
   * Only the fetch/parse is guarded so rendering bugs stay visible. The fallback
   * keeps the selector working without a manifest (e.g. a plain `zensical serve`).
   */
  function loadVersions(root) {
    const fallback = [{ version: "main", title: "main (unreleased)", aliases: [] }];
    return fetch(`${root}versions.json`)
      .then((response) => (response.ok ? response.json() : fallback))
      .then((raw) => {
        const versions = normalizeVersions(raw);
        return versions.length ? versions : fallback;
      })
      .catch((error) => {
        console.warn("version-switcher: could not load versions.json", error);
        return fallback;
      });
  }

  // ---- navigation ----

  // The target version may not have this page.
  function pageExists(url) {
    return fetch(url, { method: "HEAD" })
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

    const toggle = document.createElement("a");
    toggle.className = "md-version__current";
    toggle.href = "#";
    toggle.setAttribute("aria-label", "Select documentation version");
    const label = document.createElement("span");
    label.className = `${SWITCHER_CLASS}__label`;
    toggle.appendChild(label);

    const list = document.createElement("ul");
    list.className = "md-version__list";
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
    const nav = document.querySelector(`.${SWITCHER_CLASS}`) || createSelector();
    // On a shared page, show the last-viewed version rather than latest.
    const selected = current || preferred || latest || versions[0];

    nav.querySelector(`.${SWITCHER_CLASS}__label`).textContent = selected.title;

    const items = versions.map((entry) => {
      const link = makeVersionLink(root, entry, segment);
      link.className = "md-version__link";
      link.textContent = isLatest(entry) ? `${entry.title} (latest)` : entry.title;
      link.setAttribute("aria-current", entry === selected ? "true" : "false");
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
    rewriteSiteRootLinks(root);
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
    // Without document$, re-render if the theme swaps the header out.
    new MutationObserver(() => {
      if (!document.querySelector(`.${SWITCHER_CLASS}`)) callback();
    }).observe(document.body, { childList: true, subtree: true });
  }

  const versionsPromise = loadVersions(resolveSiteRoot());
  onPageReady(() => versionsPromise.then(render));
})();