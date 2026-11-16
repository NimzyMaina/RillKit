import { useMemo, useState } from "react";
import { ArrowDownRightIcon, ArrowUpRightIcon, MinusIcon } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { SeriesChart, StackedBars, type Point } from "@/components/charts";
import { errorMessage, errorTitle, useMetrics } from "@/api/queries";
import type { Metrics, MetricsTotals } from "@/api/types";
import { fmtBytes, fmtCompact, fmtInt, fmtRate } from "@/lib/format";
import { cn } from "@/lib/utils";

const WINDOWS = [
  { sec: 900, label: "15 min", long: "the previous 15 minutes" },
  { sec: 3600, label: "1 hour", long: "the previous hour" },
  { sec: 86400, label: "24 hours", long: "the previous 24 hours" },
];

/** Which way is better: up, down, or neither (shown, but not coloured). */
type Better = "up" | "down" | "neutral";

interface Kpi {
  label: string;
  value: string;
  sub: string;
  current: number | null;
  previous: number | null;
  better: Better;
  /** Show the change in percentage points instead of percent (for rates). */
  points?: boolean;
}

function Change({ k, against }: { k: Kpi; against: string }) {
  const { current: c, previous: p } = k;
  if (c === null || p === null || (!k.points && p === 0 && c === 0)) {
    return <span className="text-xs text-muted-foreground">no earlier data</span>;
  }
  const diff = k.points ? (c - p) * 100 : p === 0 ? Infinity : ((c - p) / Math.abs(p)) * 100;
  const flat = Math.abs(diff) < 0.5;
  const up = diff > 0;
  const good = flat || k.better === "neutral" ? null : (up && k.better === "up") || (!up && k.better === "down");
  const Icon = flat ? MinusIcon : up ? ArrowUpRightIcon : ArrowDownRightIcon;
  const amount = !Number.isFinite(diff)
    ? "new"
    : `${up ? "+" : diff < 0 ? "−" : ""}${Math.abs(diff) >= 100 ? fmtInt(Math.abs(diff)) : Math.abs(diff).toFixed(1)}${k.points ? " pts" : "%"}`;
  const verdict = good === null ? "" : good ? ", an improvement" : ", worse";
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium font-numeric",
        good === true && "bg-state-running-fill text-state-running",
        good === false && "bg-state-draining-fill text-state-draining",
        good === null && "bg-muted text-muted-foreground",
      )}
      title={`vs ${against}`}
      data-trend={good === null ? "neutral" : good ? "better" : "worse"}
    >
      <Icon className="size-3.5" aria-hidden />
      {flat ? "no change" : amount}
      <span className="sr-only">{` compared with ${against}${verdict}`}</span>
    </span>
  );
}

function kpis(m: Metrics): Kpi[] {
  const c: MetricsTotals = m.current;
  const p: MetricsTotals = m.previous;
  const hasPrev = p.succeeded + p.failed > 0;
  const prev = <T,>(v: T) => (hasPrev ? v : null);
  return [
    { label: "Messages handled", value: fmtCompact(c.succeeded), sub: `${fmtCompact(c.failed)} failed attempts`, current: c.succeeded, previous: prev(p.succeeded), better: "up" },
    {
      label: "Success rate",
      value: c.success_rate === null ? "—" : `${(c.success_rate * 100).toFixed(c.success_rate > 0.999 ? 2 : 1)}%`,
      sub: "of handler runs",
      current: c.success_rate,
      previous: prev(p.success_rate),
      better: "up",
      points: true,
    },
    { label: "Throughput", value: fmtRate(c.per_sec), sub: "messages / second", current: c.per_sec, previous: prev(p.per_sec), better: "up" },
    { label: "Data rate", value: `${fmtBytes(c.bytes_per_sec)}/s`, sub: `${fmtBytes(c.bytes)} handled`, current: c.bytes_per_sec, previous: prev(p.bytes_per_sec), better: "neutral" },
    { label: "In the DLQ", value: fmtCompact(c.dlq_length), sub: "dead-lettered, waiting", current: c.dlq_length, previous: p.dlq_length, better: "down" },
    { label: "Dead-lettered", value: fmtCompact(c.dead_lettered), sub: "in this window", current: c.dead_lettered, previous: prev(p.dead_lettered), better: "down" },
  ];
}

export function Throughput({ tenant }: { tenant: string }) {
  const [windowSec, setWindowSec] = useState(3600);
  const { data: m, isPending, error } = useMetrics(tenant, windowSec);
  const against = WINDOWS.find((w) => w.sec === windowSec)!.long;
  const bucketSec = windowSec / 60;

  const series = useMemo<Point[]>(
    () =>
      (m?.series ?? []).map((p) => {
        const ran = p.succeeded + p.failed;
        // a bucket without samples is a gap, not a fall to zero
        if (p.ticks === 0)
          return { t: Date.parse(p.ts), succeeded: null, failed: null, success: null, rate: null, dlq: null, dead: null };
        return {
          t: Date.parse(p.ts),
          succeeded: p.succeeded,
          failed: p.failed,
          success: ran > 0 ? (p.succeeded / ran) * 100 : null,
          rate: p.bytes / bucketSec,
          dlq: p.dlq_length,
          dead: p.dead_lettered,
        };
      }),
    [m, bucketSec],
  );

  return (
    <section aria-labelledby="throughput-title" className="flex flex-col gap-5">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div className="flex flex-col gap-1">
          <h2 id="throughput-title" className="font-serif text-3xl tracking-tight">
            Throughput
          </h2>
          <p className="text-sm text-muted-foreground">
            Every stream of this tenant, compared with {against}.
          </p>
        </div>
        <ToggleGroup
          type="single"
          variant="outline"
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
      </div>

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>{errorTitle(error)}</AlertTitle>
          <AlertDescription>{errorMessage(error)}</AlertDescription>
        </Alert>
      ) : isPending || !m ? (
        <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6">
          {Array.from({ length: 6 }, (_, i) => (
            <Skeleton key={i} className="h-32 rounded-[1.25rem]" />
          ))}
        </div>
      ) : (
        <>
          <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6" aria-label="Trends">
            {kpis(m).map((k) => (
              <Card key={k.label} size="sm">
                <CardHeader>
                  <CardDescription className="font-medium text-foreground/80">{k.label}</CardDescription>
                </CardHeader>
                <CardContent className="flex flex-col gap-2">
                  <span className="font-serif text-4xl leading-none tracking-tight font-numeric" data-trend-kpi={k.label}>
                    {k.value}
                  </span>
                  <span className="text-xs text-muted-foreground">{k.sub}</span>
                  <Change k={k} against={against} />
                </CardContent>
              </Card>
            ))}
          </div>

          <div className="grid gap-6 lg:grid-cols-2">
            <Card>
              <CardHeader>
                <CardTitle>Messages handled</CardTitle>
                <CardDescription>Handler runs per {bucketSec >= 60 ? `${Math.round(bucketSec / 60)} min` : `${bucketSec} s`}, succeeded and failed.</CardDescription>
              </CardHeader>
              <CardContent>
                <StackedBars
                  data={series}
                  format={fmtCompact}
                  series={[
                    { key: "succeeded", label: "Succeeded", color: "var(--chart-1)" },
                    { key: "failed", label: "Failed", color: "var(--chart-4)" },
                  ]}
                />
              </CardContent>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle>Success rate</CardTitle>
                <CardDescription>Share of handler runs that succeeded.</CardDescription>
              </CardHeader>
              <CardContent>
                <SeriesChart
                  data={series}
                  dataKey="success"
                  label="Success rate"
                  kind="line"
                  format={(v) => `${v.toFixed(v >= 99.95 || v === 0 ? 0 : 1)}%`}
                  domain={[(min: number) => Math.max(0, Math.floor(min - 2)), 100]}
                  className="h-56"
                />
              </CardContent>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle>Data rate</CardTitle>
                <CardDescription>Bytes of messages handled per second.</CardDescription>
              </CardHeader>
              <CardContent>
                <SeriesChart data={series} dataKey="rate" label="Data rate" format={(v) => `${fmtBytes(v)}/s`} axisWidth={72} className="h-56" />
              </CardContent>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle>Dead-letter queue</CardTitle>
                <CardDescription>Messages waiting in the DLQs of all streams.</CardDescription>
              </CardHeader>
              <CardContent>
                <SeriesChart data={series} dataKey="dlq" label="In the DLQ" kind="step" format={fmtCompact} className="h-56" />
              </CardContent>
            </Card>
          </div>
        </>
      )}
    </section>
  );
}
