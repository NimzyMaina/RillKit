#!/usr/bin/env node
// Build the versioned docs site.
//
//   latest   <base>            built from the current checkout (main in CI)
//   vX.Y.Z   <base>vX.Y.Z/     one per stable tag, built from the tag's commit
//                              with that commit's own docs and package-lock
//
// Output: .docs-build/site/ (override with --out), plus <base>versions.json,
// the manifest the navbar version picker reads at runtime.
//
// Snapshots are immutable, so each finished one is cached under
// .docs-build/cache/ (override with --cache) keyed by tag, commit sha, site base
// and BUILDER_VERSION; a cache hit is copied instead of rebuilt. Bump
// BUILDER_VERSION whenever the way snapshots are built changes.
//
// Any failure exits non-zero and leaves the previous --out untouched: the site
// is assembled in a temp dir and only moved into place once it is complete.
//
// Options:
//   --out <dir>        output dir                     (default .docs-build/site)
//   --cache <dir>      snapshot cache dir             (default .docs-build/cache)
//   --base <path>      site base                      (default /rillkit/)
//   --ref <tag>=<rev>  pretend <tag> points at <rev>, e.g. --ref v0.1.1=6cc2b51
//                      (repeatable; for testing without creating tags)
//   --no-tags          ignore real tags, only use --ref ones
//   --no-cache         rebuild every snapshot (still refreshes the cache)
//
// Env: RILLKIT_GH_RELEASES (comma-separated tags with a GitHub release) is
// passed to the latest build for the version history page.
import { spawnSync } from 'node:child_process'
import { createHash } from 'node:crypto'
import { cpSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, renameSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join, relative, resolve, sep } from 'node:path'
import { fileURLToPath } from 'node:url'
import { parseArgs } from 'node:util'
import { git, listStableTags } from '../docs/.vitepress/lib/versions-git.mjs'
import { MANIFEST_FILE, compareTagsDesc } from '../docs/.vitepress/lib/versions-shared.mjs'

const BUILDER_VERSION = '1'

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..')

const { values: opts } = parseArgs({
  options: {
    out: { type: 'string', default: '.docs-build/site' },
    cache: { type: 'string', default: '.docs-build/cache' },
    base: { type: 'string', default: '/rillkit/' },
    ref: { type: 'string', multiple: true, default: [] },
    'no-tags': { type: 'boolean', default: false },
    'no-cache': { type: 'boolean', default: false },
  },
})

const base = opts.base.replace(/^\/?/, '/').replace(/\/?$/, '/')
const outDir = resolve(ROOT, opts.out)
const cacheDir = resolve(ROOT, opts.cache)
const overrides = Object.fromEntries(
  opts.ref.map((r) => {
    const i = r.indexOf('=')
    if (i < 1) fail(`--ref expects <tag>=<rev>, got "${r}"`)
    return [r.slice(0, i), r.slice(i + 1)]
  }),
)

const log = (msg) => console.log(`[build-docs] ${msg}`)
function fail(msg) {
  console.error(`[build-docs] error: ${msg}`)
  process.exit(1)
}

function run(cmd, args, { cwd, env } = {}) {
  log(`$ ${cmd} ${args.join(' ')}${cwd && cwd !== ROOT ? `   (in ${cwd})` : ''}`)
  const res = spawnSync(cmd, args, { cwd: cwd ?? ROOT, env: { ...process.env, ...env }, stdio: 'inherit' })
  if (res.error) throw res.error
  if (res.status !== 0) throw new Error(`${cmd} ${args.join(' ')} exited with ${res.status ?? res.signal}`)
}

/** Every page of a built VitePress app, as paths relative to its base ('', 'guide/concepts', ...). */
function listPages(siteDir, skipDirs = new Set()) {
  const pages = []
  const walk = (dir) => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const full = join(dir, entry.name)
      const rel = relative(siteDir, full).split(sep).join('/')
      if (entry.isDirectory()) {
        if (!(dir === siteDir && skipDirs.has(entry.name))) walk(full)
      } else if (entry.name.endsWith('.html') && rel !== '404.html') {
        pages.push(rel.replace(/(^|\/)index\.html$/, '$1').replace(/\.html$/, ''))
      }
    }
  }
  walk(siteDir)
  return pages.sort()
}

/** Guard against a toolchain that ignored --base: the home page must load assets from it. */
function assertBase(siteDir, expectedBase) {
  const index = join(siteDir, 'index.html')
  if (!existsSync(index)) throw new Error(`${siteDir} has no index.html`)
  const html = readFileSync(index, 'utf8')
  if (!html.includes(`"${expectedBase}assets/`)) {
    throw new Error(`${index} does not load its assets from ${expectedBase}assets/ (was --base ignored?)`)
  }
}

const vitepressBin = (dir) => join(dir, 'node_modules', '.bin', process.platform === 'win32' ? 'vitepress.cmd' : 'vitepress')

/** RILLKIT_VERSIONS_JSON for a build: the tags its version history page lists. */
const versionsEnv = (tags) =>
  JSON.stringify(tags.map(({ tag, sha, date, docs, reason }) => ({ tag, sha, date, docs, reason })))

function buildSnapshot(t, tags, work) {
  const snapBase = `${base}${t.tag}/`
  const key = `${t.tag}-${t.sha.slice(0, 12)}-${createHash('sha256').update(`${BUILDER_VERSION}\0${base}\0${t.sha}`).digest('hex').slice(0, 12)}`
  const cached = join(cacheDir, key)
  if (!opts['no-cache'] && existsSync(join(cached, 'complete'))) {
    log(`${t.tag}: using cached snapshot ${key}`)
    return { key, site: join(cached, 'site') }
  }

  const wt = join(work, `wt-${t.tag}`)
  const out = join(work, `out-${t.tag}`)
  log(`${t.tag}: building ${t.sha.slice(0, 12)} at ${snapBase}`)
  git(['worktree', 'add', '--detach', '--force', wt, t.sha], ROOT)
  try {
    const hasLock = existsSync(join(wt, 'package-lock.json'))
    if (!hasLock) log(`${t.tag}: notice: no package-lock.json, falling back to npm install`)
    run('npm', [hasLock ? 'ci' : 'install', '--no-audit', '--no-fund'], { cwd: wt })
    if (!existsSync(vitepressBin(wt))) throw new Error(`${t.tag}: vitepress is not installed by its package.json`)
    // Snapshot content must not depend on what was released later (it is cached
    // forever): its history page only knows tags up to itself, no release notes.
    const older = tags.filter((o) => compareTagsDesc(o.tag, t.tag) >= 0)
    run(vitepressBin(wt), ['build', 'docs', '--base', snapBase, '--outDir', out], {
      cwd: wt,
      env: { RILLKIT_VERSIONS_JSON: versionsEnv(older), RILLKIT_GH_RELEASES: '' },
    })
    assertBase(out, snapBase)
  } finally {
    git(['worktree', 'remove', '--force', wt], ROOT)
  }

  rmSync(cached, { recursive: true, force: true })
  mkdirSync(cached, { recursive: true })
  renameSync(out, join(cached, 'site'))
  writeFileSync(join(cached, 'complete'), `${t.tag} ${t.sha} builder ${BUILDER_VERSION}\n`)
  return { key, site: join(cached, 'site') }
}

function main() {
  const tags = listStableTags(ROOT, { overrides, includeRealTags: !opts['no-tags'] })
  log(tags.length ? `stable tags: ${tags.map((t) => `${t.tag}@${t.sha.slice(0, 7)}`).join(', ')}` : 'no stable tags: building latest only')
  for (const t of tags.filter((t) => !t.docs)) log(`${t.tag}: notice: skipped, ${t.reason}`)

  const work = mkdtempSync(join(tmpdir(), 'rillkit-docs-'))
  const cleanup = () => {
    rmSync(work, { recursive: true, force: true })
    try {
      git(['worktree', 'prune'], ROOT)
    } catch {
      // nothing to prune
    }
  }
  process.on('SIGINT', () => {
    cleanup()
    process.exit(130)
  })

  try {
    const site = join(work, 'site')
    const latestSha = git(['rev-parse', 'HEAD'], ROOT)
    const dirty = git(['status', '--porcelain', '--', 'docs', 'package.json', 'package-lock.json'], ROOT) !== ''
    log(`latest: building the current checkout (${latestSha.slice(0, 12)}${dirty ? ', with uncommitted docs changes' : ''}) at ${base}`)
    run(vitepressBin(ROOT), ['build', 'docs', '--base', base, '--outDir', site], {
      env: { RILLKIT_VERSIONS_JSON: versionsEnv(tags) },
    })
    assertBase(site, base)

    const snapTags = tags.filter((t) => t.docs)
    const latestPages = listPages(site)
    const clash = snapTags.find((t) => existsSync(join(site, t.tag)))
    if (clash) throw new Error(`latest docs already contain ${clash.tag}/, which is where that snapshot goes`)

    mkdirSync(cacheDir, { recursive: true })
    const versions = [
      {
        id: 'latest',
        label: tags[0]?.sha === latestSha ? `latest (${tags[0].tag})` : 'latest',
        path: '',
        ref: 'main',
        sha: latestSha,
        pages: latestPages,
      },
    ]
    const usedKeys = new Set()
    for (const t of snapTags) {
      const snap = buildSnapshot(t, tags, work)
      usedKeys.add(snap.key)
      cpSync(snap.site, join(site, t.tag), { recursive: true })
      versions.push({ id: t.tag, label: t.tag, path: `${t.tag}/`, sha: t.sha, date: t.date, pages: listPages(snap.site) })
    }

    const manifest = {
      generated: new Date().toISOString(),
      base,
      historyPage: latestPages.includes('versions'),
      versions,
      skipped: tags.filter((t) => !t.docs).map(({ tag, sha, reason }) => ({ tag, sha, reason })),
    }
    writeFileSync(join(site, MANIFEST_FILE), `${JSON.stringify(manifest, null, 2)}\n`)

    rmSync(outDir, { recursive: true, force: true })
    mkdirSync(dirname(outDir), { recursive: true })
    cpSync(site, outDir, { recursive: true })

    // drop cached snapshots of tags that are gone or were built differently
    for (const entry of readdirSync(cacheDir)) {
      if (!usedKeys.has(entry)) rmSync(join(cacheDir, entry), { recursive: true, force: true })
    }

    log(`done: ${versions.map((v) => v.id).join(', ')} -> ${relative(ROOT, outDir) || outDir}`)
  } finally {
    cleanup()
  }
}

try {
  main()
} catch (err) {
  fail(err.message)
}
