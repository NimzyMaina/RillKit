import { h } from 'vue'
import DefaultTheme from 'vitepress/theme'
import type { Theme } from 'vitepress'
import '@fontsource-variable/eb-garamond/wght.css'
import '@fontsource-variable/eb-garamond/wght-italic.css'
import '@fontsource-variable/figtree/wght.css'
import '@fontsource/ibm-plex-mono/latin-400.css'
import '@fontsource/ibm-plex-mono/latin-500.css'
import '@fontsource/ibm-plex-mono/latin-600.css'
import './custom.css'
import RillHero from './components/RillHero.vue'
import RillConnect from './components/RillConnect.vue'
import RillFlow from './components/RillFlow.vue'
import RillRedis from './components/RillRedis.vue'
import RillBefore from './components/RillBefore.vue'
import RillSpec from './components/RillSpec.vue'
import RillStart from './components/RillStart.vue'
import RillControl from './components/RillControl.vue'
import RillDivider from './components/RillDivider.vue'
import VersionSwitcher from './components/VersionSwitcher.vue'

export default {
  extends: DefaultTheme,
  // version picker: in the top bar on wide screens, in the mobile menu otherwise
  Layout: () =>
    h(DefaultTheme.Layout, null, {
      'nav-bar-content-after': () => h(VersionSwitcher),
      'nav-screen-content-after': () => h(VersionSwitcher, { screen: true }),
    }),
  enhanceApp({ app }) {
    app.component('RillHero', RillHero)
    app.component('RillConnect', RillConnect)
    app.component('RillFlow', RillFlow)
    app.component('RillRedis', RillRedis)
    app.component('RillBefore', RillBefore)
    app.component('RillSpec', RillSpec)
    app.component('RillStart', RillStart)
    app.component('RillControl', RillControl)
    app.component('RillDivider', RillDivider)
  },
} satisfies Theme
