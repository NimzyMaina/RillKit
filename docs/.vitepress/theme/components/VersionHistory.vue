<script setup lang="ts">
// Version history table for docs/versions.md. The rows come from the build-time
// data loader; links to other docs versions point at separate VitePress apps,
// so they carry target="_self" to make the client router let the browser do a
// full page load instead of trying to resolve them inside this app.
import { computed } from 'vue'
import { useData } from 'vitepress'
import { data } from '../../../versions.data.mts'
import { rootBaseOf, versionIdOf } from '../../lib/versions-shared.mjs'

const { site } = useData()
const rootBase = computed(() => rootBaseOf(site.value.base))
const current = computed(() => versionIdOf(site.value.base))

const fmt = new Intl.DateTimeFormat('en', { year: 'numeric', month: 'short', day: 'numeric', timeZone: 'UTC' })
const day = (iso: string) => fmt.format(new Date(iso))
</script>

<template>
  <div class="rk-versions">
    <p v-if="current !== 'latest'" class="rk-versions-note">
      You are reading the <strong>{{ current }}</strong> docs, so this list stops at the time it was built.
      See the <a :href="`${rootBase}versions`" target="_self">current version history</a>.
    </p>

    <p v-if="data.error">The release list could not be read from git when these docs were built.</p>
    <p v-else-if="!data.versions.length">
      No tagged releases yet. The docs on this site follow the <code>main</code> branch.
    </p>

    <template v-else>
      <table>
        <thead>
          <tr>
            <th>Version</th>
            <th>Date</th>
            <th>Docs</th>
            <th>Changes</th>
            <th>Release notes</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="v in data.versions" :key="v.tag">
            <td>
              <a :href="v.tagUrl" target="_blank" rel="noreferrer"><code>{{ v.tag }}</code></a>
            </td>
            <td><time :datetime="v.date">{{ day(v.date) }}</time></td>
            <td>
              <a v-if="v.docs" :href="`${rootBase}${v.tag}/`" target="_self">{{ v.tag }} docs</a>
              <span v-else class="rk-versions-muted" :title="v.reason ?? undefined">not available</span>
            </td>
            <td>
              <a v-if="v.compareUrl" :href="v.compareUrl" target="_blank" rel="noreferrer">
                {{ v.previous }}...{{ v.tag }}
              </a>
              <span v-else class="rk-versions-muted">initial release</span>
            </td>
            <td>
              <a v-if="v.releaseUrl" :href="v.releaseUrl" target="_blank" rel="noreferrer">notes</a>
              <span v-else class="rk-versions-muted">-</span>
            </td>
          </tr>
        </tbody>
      </table>
      <p v-if="data.unreleasedUrl">
        Unreleased changes since {{ data.versions[0].tag }}:
        <a :href="data.unreleasedUrl" target="_blank" rel="noreferrer">{{ data.versions[0].tag }}...main</a>.
        The <a :href="rootBase" target="_self">latest docs</a> are built from <code>main</code>.
      </p>
    </template>
  </div>
</template>
