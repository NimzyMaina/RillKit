import { lazy, Suspense, useState } from "react";
import {
  ArrowDownToLineIcon,
  EllipsisIcon,
  GaugeIcon,
  PauseIcon,
  PlayIcon,
  ServerIcon,
  WrenchIcon,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import { Skeleton } from "@/components/ui/skeleton";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { Confirm } from "@/components/confirm";
import { ConcurrencyDialog } from "@/components/concurrency-dialog";
import { PageHeader } from "@/components/page-header";
import { StateBadge } from "@/components/state-badge";
import { useOverview, useSendDirective } from "@/api/queries";
import type { Node, Overview, WorkerState } from "@/api/types";
import { fmtCompact, fmtMb, fmtPct, fmtRate, relativeTime } from "@/lib/format";
import { inputLabel } from "@/lib/directives";
import type { DirectiveInput } from "@/api/types";
import { useNow } from "@/hooks/use-now";
import { cn } from "@/lib/utils";

// the charts library is large: load the throughput section on its own, after the page
const Throughput = lazy(() => import("./overview-throughput").then((m) => ({ default: m.Throughput })));

const HEADLINE: Record<WorkerState, [string, string, string]> = {
  RUNNING: ["Everything is ", "flowing", "."],
  PAUSED: ["Workers are ", "paused", "."],
  MAINTENANCE: ["Down for ", "maintenance", "."],
  DRAINING: ["Workers are ", "draining", "."],
};

export function OverviewPage({ tenant }: { tenant: string }) {
  const { data: ov, isPending, error } = useOverview(tenant);

  if (isPending) return <OverviewSkeleton />;
  if (error || !ov) {
    return (
      <PageHeader title="Overview" description={error ? `Could not load the overview: ${error.message}` : undefined} />
    );
  }

  const online = ov.totals.nodes_online;
  const [pre, em, post] = online === 0 ? ["No workers ", "connected", "."] : HEADLINE[ov.state];

  return (
    <div className="flex flex-col gap-10">
      <PageHeader
        eyebrow={
          <span className="flex items-center gap-2">
            {ov.tenant.name}
            <span className="font-mono text-xs">{ov.tenant.id}</span>
            {ov.tenant.enforce_keys && <Badge variant="lavender">Cloud mode</Badge>}
          </span>
        }
        title={
          <>
            {pre}
            <em className={cn("italic", ov.state === "RUNNING" && online > 0 ? "text-brand" : "")}>{em}</em>
            {post}
          </>
        }
        actions={online > 0 ? <StateBadge state={ov.state} className="h-8 px-3.5 text-sm" /> : undefined}
      />

      <Kpis ov={ov} />
      <GlobalControls tenant={tenant} ov={ov} />
      <Suspense fallback={<Skeleton className="h-96 rounded-[1.25rem]" />}>
        <Throughput tenant={tenant} />
      </Suspense>
      <NodesCard tenant={tenant} ov={ov} />
    </div>
  );
}

function Kpis({ ov }: { ov: Overview }) {
  const t = ov.totals;
  const items = [
    { label: "Nodes online", value: fmtCompact(t.nodes_online), sub: `${ov.nodes.length} seen in 10 min` },
    { label: "Active workers", value: fmtCompact(t.active_workers), sub: "lanes consuming" },
    { label: "In flight", value: fmtCompact(t.in_flight), sub: "being handled now" },
    { label: "Total lag", value: fmtCompact(t.lag), sub: `${fmtCompact(t.pending)} pending` },
    { label: "Ack rate", value: fmtRate(t.ack_rate_sec), sub: "messages / second" },
  ];
  return (
    <section aria-label="Key figures" className="grid grid-cols-2 gap-3 md:grid-cols-6 lg:grid-cols-5">
      {items.map((k, i) => (
        <Card key={k.label} size="sm" className={cn(i < 3 ? "md:col-span-2 lg:col-span-1" : "md:col-span-3 lg:col-span-1", i === 4 && "col-span-2 md:col-span-3", i === 3 && "bg-lavender/45 dark:bg-lavender/10", i === 4 && "bg-pink/25 dark:bg-pink/10")}>
          <CardHeader>
            <CardDescription className="font-medium text-foreground/80">{k.label}</CardDescription>
          </CardHeader>
          <CardContent className="flex flex-col gap-1">
            <span className="font-serif text-5xl leading-none tracking-tight font-numeric" data-kpi={k.label}>
              {k.value}
            </span>
            <span className="text-xs text-muted-foreground">{k.sub}</span>
          </CardContent>
        </Card>
      ))}
    </section>
  );
}

function GlobalControls({ tenant, ov }: { tenant: string; ov: Overview }) {
  const send = useSendDirective(tenant);
  const busy = send.isPending;
  const noNodes = ov.totals.nodes_online === 0;
  const run = (input: DirectiveInput) => send.mutate({ input, label: inputLabel(input) });
  const setState = (state: WorkerState) => run({ cmd: "SET_STATE", scope: "global", params: { state } });
  const maintenance = ov.state === "MAINTENANCE";

  return (
    <Card>
      <CardHeader>
        <CardTitle>Controls</CardTitle>
        <CardDescription>Apply to every worker of this tenant. Each change is a directive you can follow in Activity.</CardDescription>
      </CardHeader>
      <CardContent className="flex flex-wrap items-center gap-3">
        <Confirm
          title="Pause all workers?"
          description={
            <>
              <p>Every worker stops reading new messages. Messages already in flight finish and are acknowledged.</p>
              <p>Lag will grow until you resume. Nothing is lost.</p>
            </>
          }
          confirmLabel="Pause all"
          onConfirm={() => setState("PAUSED")}
          trigger={
            <Button variant="outline" disabled={busy || noNodes}>
              <PauseIcon data-icon="inline-start" />
              Pause all
            </Button>
          }
        />
        <Button variant="brand" disabled={busy || noNodes} onClick={() => setState("RUNNING")}>
          <PlayIcon data-icon="inline-start" />
          Resume
        </Button>
        <div className="flex h-9 items-center gap-3 rounded-full border px-4">
          <WrenchIcon className="size-4 text-muted-foreground" aria-hidden />
          <Label htmlFor="global-maintenance" className="font-medium">
            Maintenance
          </Label>
          <Switch
            id="global-maintenance"
            checked={maintenance}
            disabled={busy || noNodes}
            onCheckedChange={(on) => run({ cmd: on ? "MAINTENANCE_ON" : "MAINTENANCE_OFF", scope: "global" })}
          />
        </div>
        <div className="flex-1" />
        <Confirm
          destructive
          title="Drain all workers?"
          description={
            <>
              <p>
                Workers stop taking new messages, finish what they hold, acknowledge it and then stay idle. Use it
                before a deploy or shutdown.
              </p>
              <p>Consuming starts again only when you resume.</p>
            </>
          }
          confirmLabel="Drain all"
          onConfirm={() => setState("DRAINING")}
          trigger={
            <Button variant="destructive" disabled={busy || noNodes}>
              <ArrowDownToLineIcon data-icon="inline-start" />
              Drain
            </Button>
          }
        />
      </CardContent>
    </Card>
  );
}

type NodeAction = { kind: "pause" | "drain" | "concurrency"; node: Node } | null;

function NodesCard({ tenant, ov }: { tenant: string; ov: Overview }) {
  const now = useNow(5000);
  const send = useSendDirective(tenant);
  const [action, setAction] = useState<NodeAction>(null);
  const run = (input: DirectiveInput) => send.mutate({ input, label: inputLabel(input) });
  const nodeState = (node: Node, state: WorkerState) =>
    run({ cmd: "SET_STATE", scope: "node", target: { node_id: node.node_id }, params: { state } });

  return (
    <Card className="gap-2">
      <CardHeader>
        <CardTitle>Nodes</CardTitle>
        <CardDescription>Connected first, then nodes seen in the last 10 minutes.</CardDescription>
        <CardAction>
          <Badge variant="outline" className="font-mono">
            {ov.totals.nodes_online} / {ov.nodes.length}
          </Badge>
        </CardAction>
      </CardHeader>
      <CardContent className="px-0">
        {ov.nodes.length === 0 ? (
          <Empty>
            <EmptyHeader>
              <EmptyMedia variant="icon">
                <ServerIcon />
              </EmptyMedia>
              <EmptyTitle>No nodes yet</EmptyTitle>
              <EmptyDescription>Start a worker with control enabled and an API key from Settings.</EmptyDescription>
            </EmptyHeader>
          </Empty>
        ) : (
          <Table className="min-w-[56rem]">
            <TableHeader>
              <TableRow className="hover:bg-transparent">
                <TableHead className="pl-6">Node</TableHead>
                <TableHead>State</TableHead>
                <TableHead className="text-right">Workers</TableHead>
                <TableHead className="text-right">In flight</TableHead>
                <TableHead className="text-right">CPU</TableHead>
                <TableHead className="text-right">Memory</TableHead>
                <TableHead>SDK</TableHead>
                <TableHead>Last seen</TableHead>
                <TableHead className="pr-6 text-right">
                  <span className="sr-only">Actions</span>
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {ov.nodes.map((n) => (
                <TableRow key={n.node_id} className={cn(!n.connected && "text-muted-foreground")} data-node={n.node_id}>
                  <TableCell className="pl-6">
                    <span className="flex items-center gap-2.5">
                      <ConnectedDot connected={n.connected} />
                      <span className="font-mono text-[0.8rem]">{n.node_id}</span>
                    </span>
                  </TableCell>
                  <TableCell>
                    {n.connected ? <StateBadge state={n.state} /> : <Badge variant="outline">Offline</Badge>}
                  </TableCell>
                  <TableCell className="text-right font-mono font-numeric">
                    {n.active}
                    <span className="text-muted-foreground"> / {n.streams.reduce((a, s) => a + (s.concurrency ?? 0), 0)}</span>
                  </TableCell>
                  <TableCell className="text-right font-mono font-numeric">{n.in_flight}</TableCell>
                  <TableCell className="text-right font-mono font-numeric">
                    <CpuCell pct={n.cpu_pct} />
                  </TableCell>
                  <TableCell className="text-right font-mono font-numeric">{fmtMb(n.mem_mb)}</TableCell>
                  <TableCell className="font-mono text-[0.8rem]">
                    {n.sdk ? `${n.sdk.language} ${n.sdk.version}` : "—"}
                  </TableCell>
                  <TableCell>
                    <Tooltip>
                      <TooltipTrigger className="rounded underline decoration-dotted decoration-border underline-offset-4 outline-none focus-visible:ring-3 focus-visible:ring-ring/50">
                        {n.connected && now - Date.parse(n.last_seen) < 15000 ? "now" : relativeTime(n.last_seen, now)}
                      </TooltipTrigger>
                      <TooltipContent>
                        {new Date(n.last_seen).toLocaleString()}
                        {n.connected_at && ` · connected ${relativeTime(n.connected_at, now)}`}
                      </TooltipContent>
                    </Tooltip>
                  </TableCell>
                  <TableCell className="pr-6 text-right">
                    <DropdownMenu>
                      <DropdownMenuTrigger asChild>
                        <Button variant="ghost" size="icon-sm" aria-label={`Actions for ${n.node_id}`} disabled={!n.connected}>
                          <EllipsisIcon />
                        </Button>
                      </DropdownMenuTrigger>
                      <DropdownMenuContent align="end" className="w-56">
                        <DropdownMenuLabel className="font-mono">{n.node_id}</DropdownMenuLabel>
                        <DropdownMenuGroup>
                          <DropdownMenuItem onSelect={() => setAction({ kind: "pause", node: n })}>
                            <PauseIcon />
                            Pause node
                          </DropdownMenuItem>
                          <DropdownMenuItem onSelect={() => nodeState(n, "RUNNING")}>
                            <PlayIcon />
                            Resume node
                          </DropdownMenuItem>
                          <DropdownMenuItem variant="destructive" onSelect={() => setAction({ kind: "drain", node: n })}>
                            <ArrowDownToLineIcon />
                            Drain node
                          </DropdownMenuItem>
                        </DropdownMenuGroup>
                        <DropdownMenuSeparator />
                        <DropdownMenuGroup>
                          <DropdownMenuItem onSelect={() => setAction({ kind: "concurrency", node: n })}>
                            <GaugeIcon />
                            Set concurrency…
                          </DropdownMenuItem>
                        </DropdownMenuGroup>
                      </DropdownMenuContent>
                    </DropdownMenu>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </CardContent>

      <Confirm
        open={action?.kind === "pause"}
        onOpenChange={(o) => !o && setAction(null)}
        title={`Pause ${action?.node.node_id ?? "node"}?`}
        description={<p>This node stops reading new messages; in-flight messages finish. Other nodes keep consuming.</p>}
        confirmLabel="Pause node"
        onConfirm={() => action && nodeState(action.node, "PAUSED")}
      />
      <Confirm
        open={action?.kind === "drain"}
        onOpenChange={(o) => !o && setAction(null)}
        destructive
        title={`Drain ${action?.node.node_id ?? "node"}?`}
        description={
          <p>The node finishes and acknowledges what it holds, then stays idle. Use it before stopping that machine.</p>
        }
        confirmLabel="Drain node"
        onConfirm={() => action && nodeState(action.node, "DRAINING")}
      />
      {action?.kind === "concurrency" && (
        <ConcurrencyDialog
          open
          onOpenChange={(o) => !o && setAction(null)}
          title={<>Concurrency of <span className="font-mono text-xl">{action.node.node_id}</span></>}
          description="Sets how many workers (lanes) run on this node for each of its subscriptions."
          initial={Math.max(1, ...action.node.streams.map((s) => s.concurrency ?? 1))}
          onApply={(n) =>
            run({ cmd: "SET_CONCURRENCY", scope: "node", target: { node_id: action.node.node_id }, params: { concurrency: n } })
          }
        />
      )}
    </Card>
  );
}

function ConnectedDot({ connected }: { connected: boolean }) {
  return (
    <span
      role="img"
      aria-label={connected ? "Connected" : "Not connected"}
      className={cn(
        "inline-block size-2 shrink-0 rounded-full",
        connected ? "bg-brand shadow-[0_0_0_3px_var(--state-running-fill)]" : "border border-muted-foreground",
      )}
    />
  );
}

function CpuCell({ pct }: { pct: number | null }) {
  if (pct === null) return <>—</>;
  return (
    <span className="inline-flex items-center justify-end gap-2">
      <span aria-hidden className="hidden h-1 w-10 overflow-hidden rounded-full bg-surface lg:inline-block">
        <span
          className={cn("block h-full rounded-full", pct > 85 ? "bg-coral" : pct > 65 ? "bg-amber" : "bg-brand")}
          style={{ width: `${Math.min(100, pct)}%` }}
        />
      </span>
      {fmtPct(pct)}
    </span>
  );
}

function OverviewSkeleton() {
  return (
    <div className="flex flex-col gap-10 pt-10" aria-busy>
      <Skeleton className="h-16 w-2/3 rounded-2xl" />
      <div className="grid grid-cols-2 gap-3 md:grid-cols-6 lg:grid-cols-5">
        {Array.from({ length: 5 }, (_, i) => (
          <Skeleton key={i} className="h-36 rounded-[1.25rem]" />
        ))}
      </div>
      <Skeleton className="h-32 rounded-[1.25rem]" />
      <Skeleton className="h-80 rounded-[1.25rem]" />
    </div>
  );
}
