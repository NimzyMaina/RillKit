// Version helpers shared by the browser (version selector, history page), the
// build-time data loader and scripts/build-docs.mjs. No Node APIs in here: this
// file ends up in the client bundle.

export const REPO_URL = 'https://github.com/NimzyMaina/RillKit'

/** Name of the runtime manifest written at the site root by scripts/build-docs.mjs. */
export const MANIFEST_FILE = 'versions.json'

/** Only plain release tags get a docs snapshot: vX.Y.Z, no prereleases. */
export const STABLE_TAG = /^v(\d+)\.(\d+)\.(\d+)$/

/** A snapshot lives at <root base><tag>/, so its own base ends with that segment. */
const SNAPSHOT_BASE = /v\d+\.\d+\.\d+\/$/

/** Descending semver order (newest first) for stable vX.Y.Z tags. */
export function compareTagsDesc(a, b) {
  const pa = STABLE_TAG.exec(a).slice(1).map(Number)
  const pb = STABLE_TAG.exec(b).slice(1).map(Number)
  for (let i = 0; i < 3; i++) if (pa[i] !== pb[i]) return pb[i] - pa[i]
  return 0
}

/** '/RillKit/v0.1.1/' -> '/RillKit/', '/RillKit/' -> '/RillKit/'. */
export function rootBaseOf(base) {
  return base.replace(SNAPSHOT_BASE, '')
}

/** 'v0.1.1' when this app was built as a snapshot, otherwise 'latest'. */
export function versionIdOf(base) {
  const m = base.match(/(v\d+\.\d+\.\d+)\/$/)
  return m ? m[1] : 'latest'
}

/**
 * Canonical form of a page path relative to a version's base, so pages built
 * with and without cleanUrls compare equal: 'guide/foo.html' and 'guide/foo'
 * -> 'guide/foo'; 'index.html', '' -> ''; 'examples/index.html' -> 'examples/'.
 */
export function normalizePage(path) {
  let p = decodeURI(path).replace(/^\/+/, '').replace(/[?#].*$/, '')
  p = p.replace(/(^|\/)index(\.html)?$/, '$1')
  return p.replace(/\.html$/, '')
}

/**
 * Where switching to `target` should land: the same page if the target version
 * has it, otherwise the target version's home page.
 */
export function switchUrl({ rootBase, target, currentPage, hash = '' }) {
  const home = rootBase + target.path
  const page = normalizePage(currentPage)
  const hit = (target.pages || []).some((p) => normalizePage(p) === page)
  return hit ? home + page + hash : home
}

export const tagUrl = (tag) => `${REPO_URL}/releases/tag/${tag}`
export const compareUrl = (from, to) => `${REPO_URL}/compare/${from}...${to}`
