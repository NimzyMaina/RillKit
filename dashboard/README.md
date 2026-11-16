# RillKit dashboard

The web dashboard for the RillKit control plane: see nodes, streams and throughput live, with
trends against the previous window; pause, drain and scale workers; see queue wait against handler
time and follow one trace across streams; browse a stream's messages and their logs; manage its
DLQ (replay, edit and replay, delete, purge); create streams; follow directives; manage
autoscaling policies and API keys.

| Screen | What is on it |
| --- | --- |
| Overview | Fleet KPIs, global controls, Throughput (KPIs with their change: green better, coral worse; messages handled, success rate, data rate, DLQ charts), nodes |
| Streams | Search and paging; New stream |
| Stream | Tabs (in the url as `?tab=`): Statistics & state (lag/workers and wait/work charts), Messages (a snapshot "as of" a time; Live appends new messages on top; clicking a row expands its detail in place, with payload, trace waterfall and logs), DLQ (paged; select, replay, edit and replay, delete, purge; the same inline detail), Configs (workers per node, poll settings, autoscaling) |
| Activity | Every directive, paged on the server, filtered by source |
| Settings | API keys and the connection snippet |

It is a single-page React app (Vite, TypeScript, Tailwind CSS v4, shadcn/ui on Radix, TanStack
Query, Recharts). It talks only to `rillkit-server` through the HTTP API in
[`../server/API.md`](../server/API.md); `src/api/types.ts` mirrors that file.

## Run it

Requires Node 20 or later. Everything runs from this directory.

```bash
npm install
```

| Command | What it does |
| --- | --- |
| `npm run dev` | Dev server on http://localhost:5173. `/api` is proxied to the server at `http://localhost:8080`. Sign in with the server's `RILLKIT_SERVER_ADMIN_TOKEN`. |
| `npm run dev:mock` | Same, but with an in-memory fake server (`VITE_MOCK=1`, `src/api/mock.ts`): two tenants, live changing data, directives that apply after a second. Any token signs in. |
| `npm run build` | Type-checks and builds to `dist/` (base `/`). The server serves it at `/` with an SPA fallback. The mock is never part of this build. |
| `npm run lint` / `npm run typecheck` | `tsc -b` over the app, the Vite config and the tests. |
| `npm test` | Playwright smoke tests against mock mode (starts `dev:mock` on port 5174). |
| `npm run screenshots` | Saves a screenshot of every screen to `e2e/screenshots/` (gitignored). |

The first time you run the tests, install the browser: `npx playwright install chromium`.

## How it is built

- `src/api/client.ts`: a typed `fetch` client. Every request sends `Authorization: Bearer <admin token>`;
  errors become `ApiError` with the server's `{error: {code, message}}`. A 401 signs you out.
- `src/api/live.tsx`: opens the events stream (`EventSource`, with a single-use ticket from
  `POST /events/ticket`: the admin token never goes in a url) and writes
  `overview` and `directive` events into the TanStack Query cache, so screens update without
  polling. After a reconnect it refetches, since events may have been missed.
- `src/api/queries.ts`: query keys, hooks and mutations. Sending a directive opens a toast that
  follows its status (PENDING, SENT, APPLIED or the failure) as `directive` events arrive.
- `src/lib/router.tsx`: a small pathname router (`history.pushState`, `popstate`, a hook). Routes:
  `/t/{tenant}`, `/t/{tenant}/streams`, `/t/{tenant}/streams/{stream}/{group}`,
  `/t/{tenant}/activity`, `/t/{tenant}/settings`.
- `src/components/ui/`: shadcn/ui components, restyled for this design (pill buttons and badges,
  large card radii, state badge variants).
- `src/components/charts.tsx`: every chart has one y-axis (two measures get two aligned charts,
  never a dual axis), thin marks and a hover tooltip. Chart colours are the `--chart-*` tokens in
  `src/index.css`, checked for lightness, chroma, colour-blind separation and contrast in both
  themes with the dataviz validator; re-run it if you change them.
- `src/pages/stream/`: the Messages and DLQ tabs; `src/components/message-detail.tsx` expands one
  message or DLQ entry in place under its row (payload, trace, logs) and explains how to enable
  what the workers hide; `src/components/trace-view.tsx` draws the trace waterfall (total at the
  top, a shared time ruler, one offset row per attempt).
- The admin token is kept in `localStorage` (`rillkit.admin_token`) until you sign out. The
  dashboard never displays stored secrets: API keys show only their prefix, and a new key's secret
  is shown once.

## Design tokens

Defined as shadcn CSS variables in `src/index.css`; light (cream) is the default, `.dark` is the
ink theme, toggled from the top bar.

| Token | Light | Dark | Used for |
| --- | --- | --- | --- |
| `--background` | `#ffffeb` cream | `#161615` ink | Page |
| `--card` / `--popover` | `#ffffff` | `#1f1f1d` | Cards, dialogs, menus |
| `--surface` / `--border` | `#e4e4d0` | cream at 12% | Hairline borders, tracks |
| `--foreground` / `--primary` | `#1a1a1a` | `#ffffeb` | Text, primary buttons |
| `--muted-foreground` | `#444444` | `#bdbda8` | Secondary text (9.6:1 on cream) |
| `--brand` / `--ring` | `#034f46` deep green | `#6fd0bd` | RUNNING, focus rings, links, accents |
| `--lavender` | `#f0d7ff` | | Soft highlight fills, SENT |
| `--pink` | `#ffbcf2` | | Soft highlight fills |
| `--amber` | `#ffa946` | | PAUSED and MAINTENANCE fills |
| `--coral` | `#ff6c4c` | | DRAINING and destructive fills |
| `--destructive` | `#b83218` | `#ff8a70` | Destructive text (darker than coral for contrast) |

State badges use `--state-{running,paused,draining}` (text) and `-fill` (background) pairs.

Type: **EB Garamond** (regular and italic) for page titles, KPI numbers and the wordmark;
**Figtree** for the interface; **IBM Plex Mono** for ids, keys, stream keys, code and numbers in
tables, always with tabular numerals (`font-numeric`). Radius: cards 1.25rem, buttons and badges
are pills. Borders are hairlines; there are no heavy shadows, and motion is short and respects
`prefers-reduced-motion`.

## License

[FSL-1.1-ALv2](LICENSE.md), like `rillkit-server` (see its README for what that allows).
