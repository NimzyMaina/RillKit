<script setup lang="ts">
// Navbar version picker. The list comes from <root base>versions.json, written
// by scripts/build-docs.mjs at deploy time, so even an old snapshot shows every
// version published since. Each version is its own VitePress app, so switching
// uses location.assign() (a full page load) rather than the client router.
// Without a manifest (dev server, plain `docs:build`) the picker stays hidden.
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { useData } from 'vitepress'
import { MANIFEST_FILE, rootBaseOf, switchUrl, versionIdOf } from '../../lib/versions-shared.mjs'

interface ManifestVersion {
  id: string
  label: string
  path: string
  pages: string[]
}

defineProps<{ screen?: boolean }>()

const { site } = useData()
const rootBase = computed(() => rootBaseOf(site.value.base))
const current = computed(() => versionIdOf(site.value.base))

const versions = ref<ManifestVersion[]>([])
const hasHistory = ref(false)
const selected = ref(current.value)
const HISTORY = '__history__'

const options = computed(() => {
  const list = versions.value.slice()
  // a snapshot that has since been removed from the manifest still names itself
  if (list.length && !list.some((v) => v.id === current.value)) {
    list.push({ id: current.value, label: current.value, path: '', pages: [] })
  }
  return list
})

function onChange() {
  const id = selected.value
  if (id === current.value) return
  if (id === HISTORY) {
    window.location.assign(`${rootBase.value}versions`)
    return
  }
  const target = versions.value.find((v) => v.id === id)
  if (!target) return
  const currentPage = window.location.pathname.slice(site.value.base.length)
  window.location.assign(
    switchUrl({ rootBase: rootBase.value, target, currentPage, hash: window.location.hash }),
  )
}

// coming back through the back/forward cache should not leave the other version selected
const reset = () => (selected.value = current.value)

onMounted(async () => {
  window.addEventListener('pageshow', reset)
  try {
    const res = await fetch(`${rootBase.value}${MANIFEST_FILE}`, { cache: 'no-cache' })
    if (!res.ok) return
    const manifest = await res.json()
    if (Array.isArray(manifest.versions)) versions.value = manifest.versions
    hasHistory.value = Boolean(manifest.historyPage)
  } catch {
    // no manifest (local dev, plain build, offline): keep the picker hidden
  }
})

onBeforeUnmount(() => window.removeEventListener('pageshow', reset))
</script>

<template>
  <div v-if="options.length" class="rk-version" :class="screen ? 'rk-version--screen' : 'rk-version--bar'">
    <label :for="screen ? 'rk-version-screen' : 'rk-version-bar'" :class="screen ? 'rk-version-label' : 'sr-only'">
      Docs version
    </label>
    <select :id="screen ? 'rk-version-screen' : 'rk-version-bar'" v-model="selected" class="rk-version-select" @change="onChange">
      <option v-for="v in options" :key="v.id" :value="v.id">{{ v.label }}</option>
      <option v-if="hasHistory" :value="HISTORY">All versions…</option>
    </select>
  </div>
</template>
