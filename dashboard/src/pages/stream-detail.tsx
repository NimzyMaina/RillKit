import { useEffect, useState, type FormEvent } from "react";
import { ArrowLeftIcon, PauseIcon, PlayIcon, WrenchIcon } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardAction, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "@/components/ui/empty";
import { Field, FieldContent, FieldDescription, FieldGroup, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Spinner } from "@/components/ui/spinner";
import { Switch } from "@/components/ui/switch";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { Confirm } from "@/components/confirm";
import { LagChart } from "@/components/lag-chart";
import { TimeChart } from "@/components/time-chart";
import { PageHeader } from "@/components/page-header";
import { DirectiveStatusBadge, StateBadge } from "@/components/state-badge";
import { WorkersSlider } from "@/components/workers-slider";
import {
  errorMessage,
  errorTitle,
  useDeletePolicy,
  useDirective,
  useOverview,
  usePolicies,
  useSamples,
  useSavePolicy,
  useSendDirective,
} from "@/api/queries";
import type { DirectiveInput, Overview, Policy, PolicyInput, StreamSummary, WorkerState } from "@/api/types";
import { inputLabel } from "@/lib/directives";
import { fmtCompact, fmtDateTime, fmtInt, fmtMs, fmtRate, relativeTime } from "@/lib/format";
import { Link, paths, setSearchParam, useSearchParam } from "@/lib/router";
import { DlqTab } from "./stream/dlq-tab";
import { MessagesTab } from "./stream/messages-tab";

const TABS = [
  { id: "stats", label: "Statistics & state" },
  { id: "messages", label: "Messages" },
  { id: "dlq", label: "DLQ" },
  { id: "configs", label: "Configs" },
] as const;
type TabId = (typeof TABS)[number]["id"];

const WINDOWS = [
  { sec: 300, label: "5 min" },
  { sec: 900, label: "15 min" },
  { sec: 3600, label: "1 hour" },
];

interface Props {
  tenant: string;
  stream: string;
  group: string;
}

/** Sends a directive for this stream and remembers the last one, to show its live status. */
function useStreamDirective(tenant: string) {
  const send = useSendDirective(tenant);
  const [lastId, setLastId] = useState<string | null>(null);
  const last = useDirective(tenant, lastId);
  const run = (input: DirectiveInput) =>
    send.mutate({ input, label: inputLabel(input) }, { onSuccess: (d) => setLastId(d.id) });
  return { run, busy: send.isPending, last };
}

export function StreamDetailPage({ tenant, stream, group }: Props) {
  const { data: ov, isPending } = useOverview(tenant);
  const asked = useSearchParam("tab");
  const tab: TabId = TABS.some((t) => t.id === asked) ? (asked as TabId) : "stats";
  const s = ov?.streams.find((x) => x.stream === stream && x.group === group);

  const back = (
    <Link
      href={paths.streams(tenant)}
      className="inline-flex items-center gap-1.5 rounded-full text-sm text-muted-foreground outline-none hover:text-foreground focus-visible:ring-3 focus-visible:ring-ring/50"
    >
      <ArrowLeftIcon className="size-4" aria-hidden />
      All streams
    </Link>
  );

  if (isPending) {
    return (
      <div className="flex flex-col gap-8 pt-10">
        {back}
        <Skeleton className="h-16 w-1/2 rounded-2xl" />
        <Skeleton className="h-96 rounded-[1.25rem]" />
      </div>
    );
  }

  if (!s || !ov) {
    return (
      <div className="flex flex-col gap-8 pt-10">
        {back}
        <Empty>
          <EmptyHeader>
            <EmptyTitle className="font-serif text-3xl font-normal">No such stream</EmptyTitle>
            <EmptyDescription>
              Nothing reports <span className="font-mono">{stream}</span> with group{" "}
              <span className="font-mono">{group}</span>, and it was not created here.
            </EmptyDescription>
          </EmptyHeader>
        </Empty>
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-8">
      <PageHeader
        eyebrow={back}
        title={
          <>
            {s.stream}
            <span className="text-muted-foreground"> / </span>
            <em className="italic">{s.group}</em>
          </>
        }
        description={
          <span className="flex flex-wrap gap-x-4 gap-y-1 font-mono text-xs">
            {s.stream_key && <span>stream {s.stream_key}</span>}
            {s.dlq_key && <span>dlq {s.dlq_key}</span>}
            {!s.stream_key && <span>no consumers yet</span>}
          </span>
        }
        actions={
          s.nodes > 0 ? (
            <StateBadge state={s.state} className="h-8 px-3.5 text-sm" />
          ) : (
            <Badge variant="outline" className="h-8 px-3.5 text-sm">No consumers</Badge>
          )
        }
      />

      <Tabs value={tab} onValueChange={(v) => setSearchParam("tab", v === "stats" ? null : v)} className="gap-6">
        <TabsList variant="line" className="h-10 gap-6 border-b pb-0 w-full justify-start rounded-none" aria-label="Stream sections">
          {TABS.map((t) => (
            <TabsTrigger key={t.id} value={t.id} className="flex-none px-0 text-[0.95rem]">
              {t.label}
              {t.id === "dlq" && s.dlq_length ? (
                <Badge variant={s.dlq_length > 0 ? "draining" : "outline"} className="ml-1 h-5 px-2 font-numeric">
                  {fmtCompact(s.dlq_length)}
                </Badge>
              ) : null}
            </TabsTrigger>
          ))}
        </TabsList>

        <TabsContent value="stats" className="flex flex-col gap-6">
          <Stats s={s} />
          {s.nodes > 0 ? (
            <>
              <ChartCard tenant={tenant} stream={stream} group={group} />
              <TimeCard tenant={tenant} stream={stream} group={group} />
              <StateCard tenant={tenant} s={s} ov={ov} />
            </>
          ) : (
            <NoConsumers s={s} />
          )}
        </TabsContent>
        <TabsContent value="messages">
          {s.nodes > 0 ? <MessagesTab tenant={tenant} s={s} /> : <NoConsumers s={s} />}
        </TabsContent>
        <TabsContent value="dlq">
          <DlqTab tenant={tenant} s={s} />
        </TabsContent>
        <TabsContent value="configs" className="flex flex-col gap-6">
          <div className="grid gap-6 lg:grid-cols-2">
            <WorkersCard tenant={tenant} s={s} ov={ov} />
            <PollCard tenant={tenant} s={s} ov={ov} />
          </div>
          <PolicyCard tenant={tenant} s={s} />
        </TabsContent>
      </Tabs>
    </div>
  );
}

function NoConsumers({ s }: { s: StreamSummary }) {
  return (
    <Card>
      <CardContent>
        <Empty className="py-12">
          <EmptyHeader>
            <EmptyTitle className="font-serif text-2xl font-normal">No worker consumes this stream yet</EmptyTitle>
            <EmptyDescription>
              {s.created_at ? `Created ${relativeTime(s.created_at)}. ` : ""}Start a worker subscribed to{" "}
              <span className="font-mono">{s.stream}</span> with group <span className="font-mono">{s.group}</span>, and its
              statistics, messages and DLQ appear here.
            </EmptyDescription>
          </EmptyHeader>
        </Empty>
      </CardContent>
    </Card>
  );
}

function Stats({ s }: { s: StreamSummary }) {
  const items = [
    { label: "Lag", value: fmtCompact(s.lag) },
    { label: "Pending", value: fmtCompact(s.pending) },
    { label: "Ack / s", value: fmtRate(s.ack_rate_sec) },
    { label: "Workers", value: fmtInt(s.concurrency) },
    { label: "Nodes", value: fmtInt(s.nodes) },
    { label: "In the DLQ", value: fmtCompact(s.dlq_length) },
  ];
  return (
    <dl className="grid grid-cols-2 gap-x-6 gap-y-4 rounded-[1.25rem] border bg-card px-6 py-5 sm:grid-cols-3 lg:grid-cols-6">
      {items.map((i) => (
        <div key={i.label} className="flex flex-col gap-1">
          <dt className="text-xs font-medium text-muted-foreground">{i.label}</dt>
          <dd className="font-serif text-4xl leading-none tracking-tight font-numeric">{i.value}</dd>
        </div>
      ))}
    </dl>
  );
}

function ChartCard({ tenant, stream, group }: Props) {
  const [windowSec, setWindowSec] = useState(900);
  const { data, isPending, error } = useSamples(tenant, stream, group, windowSec);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Lag and workers</CardTitle>
        <CardDescription>Messages waiting in the group, against workers across all nodes. Updated live.</CardDescription>
        <CardAction>
          <ToggleGroup
            type="single"
            variant="outline"
            size="sm"
            value={String(windowSec)}
            onValueChange={(v) => v && setWindowSec(Number(v))}
            aria-label="Time window"
          >
            {WINDOWS.map((w) => (
              <ToggleGroupItem key={w.sec} value={String(w.sec)} className="px-3">
                {w.label}
              </ToggleGroupItem>
            ))}
          </ToggleGroup>
        </CardAction>
      </CardHeader>
      <CardContent>
        {isPending ? (
          <Skeleton className="h-72 md:h-80" />
        ) : error ? (
          <Alert variant="destructive">
            <AlertTitle>{errorTitle(error)}</AlertTitle>
            <AlertDescription>{errorMessage(error)}</AlertDescription>
          </Alert>
        ) : data && data.length > 1 ? (
          <LagChart samples={data} />
        ) : (
          <Empty className="h-72">
            <EmptyHeader>
              <EmptyTitle>Not enough data yet</EmptyTitle>
              <EmptyDescription>Samples are taken every 5 seconds while workers report telemetry.</EmptyDescription>
            </EmptyHeader>
          </Empty>
        )}
      </CardContent>
    </Card>
  );
}

function TimeCard({ tenant, stream, group }: Props) {
  const [windowSec, setWindowSec] = useState(900);
  const { data, isPending, error } = useSamples(tenant, stream, group, windowSec);
  const last = data && data.length ? data[data.length - 1] : undefined;

  return (
    <Card>
      <CardHeader>
        <CardTitle>Time per message</CardTitle>
        <CardDescription>
          Queue wait against handler time, per attempt. Now{" "}
          <span className="font-numeric text-foreground">{fmtMs(last?.wait_ms)}</span> waiting and{" "}
          <span className="font-numeric text-foreground">{fmtMs(last?.work_ms)}</span> working.
        </CardDescription>
        <CardAction>
          <ToggleGroup
            type="single"
            variant="outline"
            size="sm"
            value={String(windowSec)}
            onValueChange={(v) => v && setWindowSec(Number(v))}
            aria-label="Time window"
          >
            {WINDOWS.map((w) => (
              <ToggleGroupItem key={w.sec} value={String(w.sec)} className="px-3">
                {w.label}
              </ToggleGroupItem>
            ))}
          </ToggleGroup>
        </CardAction>
      </CardHeader>
      <CardContent>
        {isPending ? (
          <Skeleton className="h-72 md:h-80" />
        ) : error ? (
          <Alert variant="destructive">
            <AlertTitle>{errorTitle(error)}</AlertTitle>
            <AlertDescription>{errorMessage(error)}</AlertDescription>
          </Alert>
        ) : data && data.length > 1 ? (
          <TimeChart samples={data} />
        ) : (
          <Empty className="h-72">
            <EmptyHeader>
              <EmptyTitle>Not enough data yet</EmptyTitle>
              <EmptyDescription>Samples are taken every 5 seconds while workers report telemetry.</EmptyDescription>
            </EmptyHeader>
          </Empty>
        )}
      </CardContent>
    </Card>
  );
}

function LastDirective({ d }: { d: ReturnType<typeof useDirective> }) {
  if (!d) return null;
  return (
    <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground" aria-live="polite">
      <span>Last change</span>
      <DirectiveStatusBadge status={d.status} />
      {d.sent_to && <span className="font-mono">{d.sent_to}</span>}
      {d.ack?.error && <span className="text-destructive">{d.ack.error}</span>}
    </div>
  );
}

function StateCard({ tenant, s, ov }: { tenant: string; s: StreamSummary; ov: Overview }) {
  const { run, busy, last } = useStreamDirective(tenant);
  const target = { stream: s.stream, group: s.group };
  const setState = (state: WorkerState) => run({ cmd: "SET_STATE", scope: "stream", target, params: { state } });

  return (
    <Card>
      <CardHeader>
        <CardTitle>State</CardTitle>
        <CardDescription>Applies to this stream on every node that consumes it.</CardDescription>
      </CardHeader>
      <CardContent className="flex flex-wrap items-center gap-3">
        {s.state === "PAUSED" || s.state === "DRAINING" ? (
          <Button variant="brand" disabled={busy} onClick={() => setState("RUNNING")}>
            <PlayIcon data-icon="inline-start" />
            Resume
          </Button>
        ) : (
          <Confirm
            title={`Pause ${s.stream}?`}
            description={
              <p>
                Workers stop reading <span className="font-mono">{s.stream}</span> for group{" "}
                <span className="font-mono">{s.group}</span>. Messages in flight finish; lag grows until you resume.
              </p>
            }
            confirmLabel="Pause stream"
            onConfirm={() => setState("PAUSED")}
            trigger={
              <Button variant="outline" disabled={busy} data-action="pause-stream">
                <PauseIcon data-icon="inline-start" />
                Pause
              </Button>
            }
          />
        )}
        {s.state === "MAINTENANCE" ? (
          <Button variant="outline" disabled={busy} onClick={() => run({ cmd: "MAINTENANCE_OFF", scope: "stream", target })}>
            <WrenchIcon data-icon="inline-start" />
            End maintenance
          </Button>
        ) : (
          <Button variant="outline" disabled={busy} onClick={() => run({ cmd: "MAINTENANCE_ON", scope: "stream", target })}>
            <WrenchIcon data-icon="inline-start" />
            Maintenance
          </Button>
        )}
        {s.state !== "PAUSED" && s.state !== "DRAINING" && s.state !== "RUNNING" && (
          <Button variant="ghost" disabled={busy} onClick={() => setState("RUNNING")}>
            <PlayIcon data-icon="inline-start" />
            Resume
          </Button>
        )}
      </CardContent>
      <CardContent className="flex flex-1 flex-col">
        <ul className="flex flex-col divide-y rounded-2xl border" aria-label="State on each node">
          {nodesOf(ov, s).map(({ node, sub }) => (
            <li key={node.node_id} className="flex items-center justify-between gap-3 px-4 py-2.5">
              <span className="font-mono text-[0.8rem]">{node.node_id}</span>
              <StateBadge state={sub.state} />
            </li>
          ))}
        </ul>
      </CardContent>
      {last && (
        <CardFooter>
          <LastDirective d={last} />
        </CardFooter>
      )}
    </Card>
  );
}

function nodesOf(ov: Overview, s: StreamSummary) {
  return ov.nodes
    .filter((n) => n.connected)
    .flatMap((n) =>
      n.streams.filter((x) => x.stream === s.stream && x.group === s.group).map((x) => ({ node: n, sub: x })),
    );
}

function WorkersCard({ tenant, s, ov }: { tenant: string; s: StreamSummary; ov: Overview }) {
  const { run, busy, last } = useStreamDirective(tenant);
  const subs = nodesOf(ov, s);
  const current = subs[0]?.sub.concurrency ?? 1;
  const [value, setValue] = useState(current);
  const [touched, setTouched] = useState(false);
  useEffect(() => {
    if (!touched) setValue(current);
  }, [current, touched]);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Workers per node</CardTitle>
        <CardDescription>
          Lanes for this stream <strong className="font-semibold text-foreground">on each node</strong>. With{" "}
          {s.nodes} {s.nodes === 1 ? "node" : "nodes"}, {value} per node is {value * s.nodes} in total.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-5">
        <Field>
          <FieldLabel htmlFor="stream-concurrency" className="sr-only">
            Workers per node
          </FieldLabel>
          <WorkersSlider
            id="stream-concurrency"
            value={value}
            onChange={(n) => {
              setTouched(true);
              setValue(n);
            }}
          />
        </Field>
        <div className="flex flex-wrap gap-1.5">
          {subs.map(({ node, sub }) => (
            <Badge key={node.node_id} variant="outline" className="font-mono">
              {node.node_id} · {sub.concurrency ?? "—"}
            </Badge>
          ))}
        </div>
        {s.policy?.enabled && (
          <p className="text-xs text-muted-foreground">
            Autoscaling is on for this stream and may change this again.
          </p>
        )}
      </CardContent>
      <CardFooter className="justify-between gap-4">
        <LastDirective d={last} />
        <Button
          className="ml-auto"
          disabled={busy || s.nodes === 0}
          onClick={() => {
            setTouched(false);
            run({ cmd: "SET_CONCURRENCY", scope: "stream", target: { stream: s.stream, group: s.group }, params: { concurrency: value } });
          }}
        >
          Apply {value} per node
        </Button>
      </CardFooter>
    </Card>
  );
}

function PollCard({ tenant, s, ov }: { tenant: string; s: StreamSummary; ov: Overview }) {
  const { run, busy, last } = useStreamDirective(tenant);
  const node = nodesOf(ov, s)[0]?.node;
  const [blockMs, setBlockMs] = useState("");
  const [batch, setBatch] = useState("");

  function submit(e: FormEvent) {
    e.preventDefault();
    const params: Record<string, number> = {};
    if (blockMs.trim()) params.block_ms = Number(blockMs);
    if (batch.trim()) params.batch_size = Number(batch);
    if (Object.keys(params).length === 0) return;
    run({ cmd: "SET_POLL_CONFIG", scope: "stream", target: { stream: s.stream, group: s.group }, params });
  }

  return (
    <Card>
      <form onSubmit={submit} className="flex flex-1 flex-col gap-(--card-spacing)">
        <CardHeader>
          <CardTitle>Poll settings</CardTitle>
          <CardDescription>How long a read blocks and how many messages it fetches. Leave a field empty to keep it.</CardDescription>
        </CardHeader>
        <CardContent className="flex-1">
          <FieldGroup className="grid grid-cols-2 gap-4">
            <Field>
              <FieldLabel htmlFor="block-ms">Block (ms)</FieldLabel>
              <Input
                id="block-ms"
                type="number"
                min={1}
                max={60000}
                inputMode="numeric"
                placeholder={node?.poll_ms != null ? String(node.poll_ms) : "2000"}
                value={blockMs}
                onChange={(e) => setBlockMs(e.target.value)}
                className="font-mono"
              />
            </Field>
            <Field>
              <FieldLabel htmlFor="batch-size">Batch size</FieldLabel>
              <Input
                id="batch-size"
                type="number"
                min={1}
                inputMode="numeric"
                placeholder={node?.batch_size != null ? String(node.batch_size) : "32"}
                value={batch}
                onChange={(e) => setBatch(e.target.value)}
                className="font-mono"
              />
            </Field>
          </FieldGroup>
        </CardContent>
        <CardFooter className="justify-between gap-4">
          <LastDirective d={last} />
          <Button type="submit" variant="outline" className="ml-auto" disabled={busy || (!blockMs.trim() && !batch.trim())}>
            Apply poll settings
          </Button>
        </CardFooter>
      </form>
    </Card>
  );
}

const DEFAULT_POLICY: PolicyInput = {
  enabled: true,
  min_workers: 1,
  max_workers: 8,
  target_lag_per_worker: 500,
  upscale_cooldown_sec: 30,
  downscale_cooldown_sec: 120,
};

function toInput(p: Policy | null | undefined): PolicyInput {
  if (!p) return DEFAULT_POLICY;
  return {
    enabled: p.enabled,
    min_workers: p.min_workers,
    max_workers: p.max_workers,
    target_lag_per_worker: p.target_lag_per_worker,
    upscale_cooldown_sec: p.upscale_cooldown_sec,
    downscale_cooldown_sec: p.downscale_cooldown_sec,
  };
}

function PolicyCard({ tenant, s }: { tenant: string; s: StreamSummary }) {
  const { data: policies, isPending } = usePolicies(tenant);
  const policy = policies?.find((p) => p.stream === s.stream && p.group === s.group) ?? null;
  // The overview carries the freshest last_decision (the autoscaler updates it).
  const live = s.policy ?? policy;
  const save = useSavePolicy(tenant, s.stream, s.group);
  const remove = useDeletePolicy(tenant, s.stream, s.group);
  const [form, setForm] = useState<PolicyInput>(() => toInput(policy));
  const [loadedFor, setLoadedFor] = useState<string | null>(null);
  const [problem, setProblem] = useState<string | null>(null);

  // Load the stored policy into the form once it arrives (and after saving or removing).
  const sig = policy ? JSON.stringify(toInput(policy)) : "none";
  useEffect(() => {
    if (isPending || loadedFor === sig) return;
    setForm(toInput(policy));
    setLoadedFor(sig);
  }, [isPending, sig, loadedFor, policy]);

  const set = <K extends keyof PolicyInput>(k: K, v: PolicyInput[K]) => setForm((f) => ({ ...f, [k]: v }));
  const num = (k: Exclude<keyof PolicyInput, "enabled">) => ({
    value: Number.isFinite(form[k]) ? String(form[k]) : "",
    onChange: (e: React.ChangeEvent<HTMLInputElement>) => set(k, e.target.value === "" ? NaN : Number(e.target.value)),
  });

  function submit(e: FormEvent) {
    e.preventDefault();
    const f = form;
    if (![f.min_workers, f.max_workers, f.target_lag_per_worker, f.upscale_cooldown_sec, f.downscale_cooldown_sec].every(Number.isFinite))
      return setProblem("Fill in every number.");
    if (f.min_workers < 0) return setProblem("Min workers must be 0 or more.");
    if (f.max_workers < f.min_workers) return setProblem("Max workers must be at least min workers.");
    if (f.max_workers > 100000) return setProblem("Max workers is at most 100,000.");
    if (f.target_lag_per_worker <= 0) return setProblem("Target lag per worker must be more than 0.");
    if (f.upscale_cooldown_sec < 0 || f.downscale_cooldown_sec < 0) return setProblem("Cooldowns cannot be negative.");
    setProblem(null);
    save.mutate(f);
  }

  return (
    <Card>
      <form onSubmit={submit} className="flex flex-col gap-(--card-spacing)">
        <CardHeader>
          <CardTitle>Autoscaling</CardTitle>
          <CardDescription>
            Wanted workers in total = lag ÷ target lag per worker (rounded up), kept between min and max, then
            spread evenly over the nodes consuming this stream (at least 1 per node). The server sends the change as
            a SET_CONCURRENCY directive.
          </CardDescription>
          <CardAction>
            {policy ? (
              <Badge variant={policy.enabled ? "running" : "outline"}>{policy.enabled ? "Active" : "Disabled"}</Badge>
            ) : (
              <Badge variant="outline">Not set</Badge>
            )}
          </CardAction>
        </CardHeader>
        <CardContent className="flex flex-col gap-6">
          {isPending ? (
            <Skeleton className="h-40" />
          ) : (
            <FieldGroup>
              <Field orientation="horizontal" className="rounded-2xl border bg-muted/40 p-4">
                <FieldContent>
                  <FieldLabel htmlFor="policy-enabled">Enabled</FieldLabel>
                  <FieldDescription>When off, the policy is kept but nothing is scaled.</FieldDescription>
                </FieldContent>
                <Switch id="policy-enabled" checked={form.enabled} onCheckedChange={(v) => set("enabled", v)} />
              </Field>
              <div className="grid grid-cols-2 gap-4 md:grid-cols-5">
                <Field>
                  <FieldLabel htmlFor="p-min">Min workers</FieldLabel>
                  <Input id="p-min" type="number" min={0} max={256} className="font-mono" {...num("min_workers")} />
                </Field>
                <Field>
                  <FieldLabel htmlFor="p-max">Max workers</FieldLabel>
                  <Input id="p-max" type="number" min={0} max={256} className="font-mono" {...num("max_workers")} />
                </Field>
                <Field>
                  <FieldLabel htmlFor="p-target">Target lag / worker</FieldLabel>
                  <Input id="p-target" type="number" min={1} className="font-mono" {...num("target_lag_per_worker")} />
                </Field>
                <Field>
                  <FieldLabel htmlFor="p-up">Scale-up cooldown (s)</FieldLabel>
                  <Input id="p-up" type="number" min={0} className="font-mono" {...num("upscale_cooldown_sec")} />
                </Field>
                <Field>
                  <FieldLabel htmlFor="p-down">Scale-down cooldown (s)</FieldLabel>
                  <Input id="p-down" type="number" min={0} className="font-mono" {...num("downscale_cooldown_sec")} />
                </Field>
              </div>
              {problem && <p className="text-sm text-destructive" role="alert">{problem}</p>}
            </FieldGroup>
          )}
          {live?.last_decision && (
            <div className="flex flex-wrap items-center gap-x-3 gap-y-1 rounded-2xl bg-lavender/40 px-4 py-3 text-sm dark:bg-lavender/10">
              <span className="font-medium">Last decision</span>
              <span className="font-mono">{live.last_decision}</span>
              {live.last_scaled_at && (
                <span className="text-muted-foreground" title={fmtDateTime(live.last_scaled_at)}>
                  {relativeTime(live.last_scaled_at)}
                </span>
              )}
            </div>
          )}
        </CardContent>
        <CardFooter className="justify-end gap-2">
          {policy && (
            <Confirm
              destructive
              title="Remove the autoscaling policy?"
              description={<p>Workers keep their current concurrency; nothing scales them until you add a policy again.</p>}
              confirmLabel="Remove policy"
              onConfirm={() => remove.mutate()}
              trigger={
                <Button type="button" variant="ghost" disabled={remove.isPending}>
                  Remove
                </Button>
              }
            />
          )}
          <Button type="submit" disabled={save.isPending || isPending}>
            {save.isPending && <Spinner data-icon="inline-start" />}
            {policy ? "Save policy" : "Create policy"}
          </Button>
        </CardFooter>
      </form>
    </Card>
  );
}
