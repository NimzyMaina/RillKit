// Node-only helpers that read release tags from git. Used by the versions data
// loader (docs/versions.data.mts) and by scripts/build-docs.mjs.
import { execFileSync } from 'node:child_process'
import { STABLE_TAG, compareTagsDesc } from './versions-shared.mjs'

export function git(args, cwd) {
  return execFileSync('git', args, { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim()
}

function commitInfo(rev, cwd) {
  const sha = git(['rev-parse', '--verify', `${rev}^{commit}`], cwd)
  const date = git(['log', '-1', '--format=%cI', sha], cwd)
  return { sha, date }
}

/** True when `sha` has what a docs build needs: a docs/ dir and a package.json. */
export function docsStatus(sha, cwd) {
  const has = (path) => {
    try {
      git(['cat-file', '-e', `${sha}:${path}`], cwd)
      return true
    } catch {
      return false
    }
  }
  if (!has('docs')) return { docs: false, reason: 'no docs/ directory at this tag' }
  if (!has('package.json')) return { docs: false, reason: 'no package.json at this tag' }
  return { docs: true, reason: null }
}

/**
 * Stable release tags (vX.Y.Z), newest first:
 *   [{ tag, sha, date, docs, reason }]
 * `sha` is the commit the tag points at (annotated tags are peeled); `date` is
 * the tag's creator date (tagger date for annotated tags, commit date for
 * lightweight ones).
 *
 * `overrides` ({ tag: rev }) pretends extra tags exist (or repoints real ones),
 * so a historical build can be tested without creating tags. `includeRealTags`
 * false limits the result to the overrides.
 */
export function listStableTags(cwd, { overrides = {}, includeRealTags = true } = {}) {
  const found = new Map()
  if (includeRealTags) {
    const out = git(
      ['for-each-ref', 'refs/tags', '--format=%(refname:short)%09%(objectname)%09%(*objectname)%09%(creatordate:iso-strict)'],
      cwd,
    )
    for (const line of out.split('\n').filter(Boolean)) {
      const [tag, obj, peeled, date] = line.split('\t')
      if (STABLE_TAG.test(tag)) found.set(tag, { tag, sha: peeled || obj, date })
    }
  }
  for (const [tag, rev] of Object.entries(overrides)) {
    if (!STABLE_TAG.test(tag)) throw new Error(`--ref ${tag}=${rev}: ${tag} is not a stable vX.Y.Z tag name`)
    found.set(tag, { tag, ...commitInfo(rev, cwd) })
  }
  return [...found.values()]
    .sort((a, b) => compareTagsDesc(a.tag, b.tag))
    .map((t) => ({ ...t, ...docsStatus(t.sha, cwd) }))
}

/** Tags that have a GitHub release, from RILLKIT_GH_RELEASES ("v0.1.0,v0.1.1"). */
export function releasedTagsFromEnv(env = process.env) {
  return new Set((env.RILLKIT_GH_RELEASES || '').split(/[\s,]+/).filter(Boolean))
}
