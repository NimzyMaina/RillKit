// Build-time data for the version history page (docs/versions.md).
//
// scripts/build-docs.mjs passes the exact tag list it is building through
// RILLKIT_VERSIONS_JSON, so the page matches the published snapshots (and
// honours its --ref test overrides). A plain `vitepress build docs` / dev
// server falls back to reading tags from git directly. RILLKIT_GH_RELEASES
// (comma-separated tag names) marks which tags have a GitHub release.
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { defineLoader } from 'vitepress'
import { listStableTags, releasedTagsFromEnv } from './.vitepress/lib/versions-git.mjs'
import { REPO_URL, compareUrl, tagUrl } from './.vitepress/lib/versions-shared.mjs'

export interface VersionRow {
  tag: string
  sha: string
  date: string
  /** A docs snapshot exists for this tag at /<root base><tag>/. */
  docs: boolean
  /** Why there is no snapshot (tag predates the docs, ...). */
  reason: string | null
  tagUrl: string
  releaseUrl: string | null
  previous: string | null
  compareUrl: string | null
}

export interface VersionsData {
  versions: VersionRow[]
  /** Compare from the newest release to main, or null without releases. */
  unreleasedUrl: string | null
  repoUrl: string
  /** Set when git could not be read; the page then shows no table. */
  error: string | null
}

interface TagInfo {
  tag: string
  sha: string
  date: string
  docs: boolean
  reason: string | null
}

declare const data: VersionsData
export { data }

function readTags(): TagInfo[] {
  if (process.env.RILLKIT_VERSIONS_JSON) return JSON.parse(process.env.RILLKIT_VERSIONS_JSON)
  return listStableTags(dirname(fileURLToPath(import.meta.url)))
}

export default defineLoader({
  load(): VersionsData {
    let tags: TagInfo[]
    try {
      tags = readTags()
    } catch (err) {
      console.warn(`[versions] could not read release tags: ${(err as Error).message}`)
      return { versions: [], unreleasedUrl: null, repoUrl: REPO_URL, error: String((err as Error).message) }
    }
    const released = releasedTagsFromEnv()
    const versions = tags.map((t, i): VersionRow => {
      const previous = tags[i + 1]?.tag ?? null
      return {
        tag: t.tag,
        sha: t.sha,
        date: t.date,
        docs: t.docs,
        reason: t.reason,
        tagUrl: tagUrl(t.tag),
        releaseUrl: released.has(t.tag) ? tagUrl(t.tag) : null,
        previous,
        compareUrl: previous ? compareUrl(previous, t.tag) : null,
      }
    })
    return {
      versions,
      unreleasedUrl: versions.length ? compareUrl(versions[0].tag, 'main') : null,
      repoUrl: REPO_URL,
      error: null,
    }
  },
})
