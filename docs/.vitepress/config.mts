import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'RillKit',
  description: 'Publish and consume Redis and Valkey streams in Python, with consumer groups, retries and a dead-letter queue',
  base: '/RillKit/',
  cleanUrls: true,
  lastUpdated: true,

  head: [['link', { rel: 'icon', href: '/RillKit/logo.svg' }]],

  markdown: {
    // Vitesse: muted greens and ambers that sit well on the cream and ink surfaces
    theme: { light: 'vitesse-light', dark: 'vitesse-dark' },
    lineNumbers: true,
    config(md) {
      // Put the file name (the `[name.py]` after the language) in an editor tab above every block.
      // VitePress strips it from standalone blocks, so read it before the stock wrapper does.
      const inner = md.renderer.rules.fence!
      const kinds: Record<string, string> = { text: 'output', bash: 'terminal', sh: 'terminal', shell: 'terminal', console: 'terminal' }
      md.renderer.rules.fence = (tokens, idx, options, env, self) => {
        const info = tokens[idx].info
        const title = info.match(/\[(.*)\]/)?.[1]
        const lang = info.trim().split(/[\s{[:]/)[0] || 'text'
        const html = inner(tokens, idx, options, env, self)
        const label = md.utils.escapeHtml(title || kinds[lang] || lang)
        return html.replace('<span class="lang">', `<span class="rk-file" data-kind="${title ? 'file' : 'lang'}">${label}</span><span class="lang">`)
      }
    },
  },

  themeConfig: {
    logo: '/logo.svg',
    nav: [
      { text: 'Guide', link: '/guide/getting-started', activeMatch: '/guide/(?!(server|dashboard|control-plane))' },
      { text: 'Examples', link: '/examples/', activeMatch: '/examples/' },
      { text: 'Control plane', link: '/guide/server', activeMatch: '/guide/(server|dashboard|control-plane)' },
      { text: 'Reference', link: '/reference/broker', activeMatch: '/reference/' },
      { text: 'PyPI', link: 'https://pypi.org/project/rillkit/' },
      { text: 'GitHub', link: 'https://github.com/NimzyMaina/RillKit' },
    ],

    sidebar: {
      '/guide/': [
        {
          text: 'Start here',
          items: [
            { text: 'Getting started', link: '/guide/getting-started' },
            { text: 'Concepts', link: '/guide/concepts' },
          ],
        },
        {
          text: 'Using RillKit',
          items: [
            { text: 'Publishing', link: '/guide/publishing' },
            { text: 'Consuming', link: '/guide/consuming' },
            { text: 'Ack policies', link: '/guide/ack-policies' },
            { text: 'Retries and the DLQ', link: '/guide/retries-and-dlq' },
            { text: 'Middleware', link: '/guide/middleware' },
            { text: 'Pydantic models', link: '/guide/pydantic' },
          ],
        },
        {
          text: 'Running it',
          items: [
            { text: 'Lifecycle', link: '/guide/lifecycle' },
            { text: 'Configuration', link: '/guide/configuration' },
            { text: 'Keys', link: '/guide/keys' },
            { text: 'Redis and Valkey Cluster', link: '/guide/cluster' },
            { text: 'Operations', link: '/guide/operations' },
            { text: 'Performance', link: '/guide/performance' },
            { text: 'Testing', link: '/guide/testing' },
            { text: 'Upgrading', link: '/guide/upgrading' },
          ],
        },
        {
          text: 'Control plane',
          items: [
            { text: 'Connecting workers', link: '/guide/control-plane' },
            { text: 'Running the server', link: '/guide/server' },
            { text: 'The dashboard', link: '/guide/dashboard' },
          ],
        },
      ],
      '/examples/': [
        {
          text: 'Examples',
          items: [
            { text: 'Overview', link: '/examples/' },
            { text: 'Basic worker', link: '/examples/basic-worker' },
            { text: 'Two-stage pipeline', link: '/examples/pipeline' },
            { text: 'Pydantic models', link: '/examples/pydantic-models' },
            { text: 'Retries and DLQ', link: '/examples/retries-and-dlq' },
            { text: 'Reject vs nack', link: '/examples/reject-vs-nack' },
            { text: 'Manual ack', link: '/examples/manual-ack' },
            { text: 'Timing and idempotency', link: '/examples/middleware-timing-and-idempotency' },
            { text: 'Multiple workers', link: '/examples/multiple-workers' },
            { text: 'Graceful shutdown', link: '/examples/graceful-shutdown' },
            { text: 'FastAPI publisher', link: '/examples/fastapi-publisher' },
            { text: 'Migrating from redis-py', link: '/examples/migrating-from-raw-redis-py' },
          ],
        },
      ],
      '/reference/': [
        {
          text: 'API reference',
          items: [
            { text: 'Broker', link: '/reference/broker' },
            { text: 'Stream', link: '/reference/stream' },
            { text: 'Message', link: '/reference/message' },
            { text: 'AckPolicy', link: '/reference/ack-policy' },
            { text: 'Middleware', link: '/reference/middleware' },
            { text: 'Settings', link: '/reference/settings' },
            { text: 'Control', link: '/reference/control' },
            { text: 'Key builders', link: '/reference/keys' },
            { text: 'Exceptions', link: '/reference/exceptions' },
          ],
        },
      ],
    },

    search: { provider: 'local' },

    editLink: {
      pattern: 'https://github.com/NimzyMaina/RillKit/edit/main/docs/:path',
      text: 'Edit this page on GitHub',
    },

    socialLinks: [
      { icon: 'github', link: 'https://github.com/NimzyMaina/RillKit' },
    ],

    footer: {
      message: 'The RillKit SDKs are released under the Apache License 2.0.',
      copyright: 'Copyright © Nimrod Maina',
    },

    outline: [2, 3],
  },
})
