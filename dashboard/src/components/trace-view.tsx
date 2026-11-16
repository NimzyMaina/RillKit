import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "@/components/ui/empty";
import { Skeleton } from "@/components/ui/skeleton";
import { errorMessage, errorTitle, useTrace } from "@/api/queries";
import type { TraceRecord } from "@/api/types";
import { fmtMs, fmtTime } from "@/lib/format";
import { cn } from "@/lib/utils";

const BAR = ["bg-chart-1", "bg-chart-2", "bg-chart-3", "bg-chart-4", "bg-chart-5"];

function colorOf(stream: string): string {
  let hash = 0;
  for (const ch of stream) hash = (hash + ch.charCodeAt(0)) % BAR.length;
  return BAR[hash];
}

const OUTCOME = {
  ok: { label: "Handled", variant: "running" as const },
  error: { label: "Failed", variant: "paused" as const },
  dead_letter: { label: "Dead-lettered", variant: "draining" as const },
};

/** Faint quarter lines behind every bar, so offsets are readable at a glance. */
const GRID = {
  backgroundImage:
    "repeating-linear-gradient(to right, var(--border) 0 1px, transparent 1px 25%)",
};

const AXIS = [0, 0.25, 0.5, 0.75, 1];

interface Step {
  record: TraceRecord;
  depth: number;
  gap: number;
  wait: number;
  work: number;
  total: number;
  offset: number;
  color: string;
}

function steps(records: TraceRecord[], from: number): Step[] {
  return records.map((record, i) => {
    const previous = records[i - 1];
    const wait = Math.max(0, record.started_at - record.published_at);
    const work = Math.max(0, record.finished_at - record.started_at);
    return {
      record,
      // one indentation step per hop, so a trace reads as a cascade like OpenObserve's tree
      depth: Math.min(i, 6),
      gap: previous ? Math.max(0, record.published_at - previous.finished_at) : 0,
      wait,
      work,
      total: wait + work,
      offset: Math.max(0, record.published_at - from),
      color: colorOf(record.stream),
    };
  });
}

/**
 * One trace, end to end, in the shape of a trace waterfall: the whole duration at the top, a
 * shared time ruler, then one row per handler attempt offset by the time it started, with the
 * queue wait pale and the handler time solid. Reads like OpenObserve's waterfall.
 */
export function TraceView({
  tenant,
  traceId,
}: {
  tenant: string;
  traceId: string;
}) {
  const { data, isPending, error } = useTrace(tenant, traceId);
  const records = data?.records ?? [];
  const from = records.length ? Math.min(...records.map((r) => r.published_at)) : 0;
  const to = records.length ? Math.max(...records.map((r) => r.finished_at)) : 0;
  const span = Math.max(1, to - from);
  const rows = steps(records, from);
  const failures = records.filter((r) => r.outcome !== "ok").length;
  const streams = new Set(records.map((r) => r.stream)).size;
  const pct = (ms: number) => Math.min(100, Math.max(0, (ms / span) * 100));

  return (
    <section className="flex flex-col gap-4" data-testid="trace-view" aria-label="Trace">
      <div className="flex min-w-0 flex-col gap-1">
        <h4 className="text-sm font-medium">Trace waterfall</h4>
        <p className="font-mono text-[0.7rem] break-all text-muted-foreground">{traceId}</p>
        {records.length > 0 && (
          <p className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
            <span className="font-numeric">
              {records.length} {records.length === 1 ? "attempt" : "attempts"}
            </span>
            <span className={cn(failures > 0 && "text-destructive")}>
              {failures > 0 ? `${failures} failed` : "no errors"}
            </span>
            <span>started {fmtTime(from)}</span>
            {data?.node_id && (
              <span>
                answered by <span className="font-mono">{data.node_id}</span>
              </span>
            )}
          </p>
        )}
      </div>

      {isPending ? (
        <div className="flex flex-col gap-2">
          {Array.from({ length: 3 }, (_, i) => (
            <Skeleton key={i} className="h-12 rounded-xl" />
          ))}
        </div>
      ) : error ? (
        <Alert variant="destructive">
          <AlertTitle>{errorTitle(error)}</AlertTitle>
          <AlertDescription>{errorMessage(error)}</AlertDescription>
        </Alert>
      ) : records.length === 0 ? (
        <Empty className="py-8">
          <EmptyHeader>
            <EmptyTitle>No records for this trace</EmptyTitle>
            <EmptyDescription>
              Workers keep a trace for an hour after its last attempt, and only with{" "}
              <code className="rounded-md bg-muted px-1.5 py-0.5 font-mono text-[0.75rem] text-foreground">
                RILLKIT_TRACE_RECORDS=true
              </code>
              . An old or untracked trace has none.
            </EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : (
        <>
          <div className="overflow-x-auto rounded-2xl border bg-card">
            <div className="min-w-[700px] p-4">
              <div className="grid grid-cols-[minmax(240px,320px)_1fr_104px] items-start gap-x-4">
                {/* ruler */}
                <div />
                <div className="relative h-6 border-b">
                  {AXIS.map((f) => (
                    <span
                      key={f}
                      className="absolute top-0 text-[0.65rem] font-numeric text-muted-foreground"
                      style={{
                        left: `${f * 100}%`,
                        transform: f === 0 ? undefined : f === 1 ? "translateX(-100%)" : "translateX(-50%)",
                      }}
                    >
                      {f === 0 ? "0" : fmtMs(span * f)}
                    </span>
                  ))}
                </div>
                <div className="text-right text-[0.6rem] tracking-wide text-muted-foreground uppercase">
                  took
                </div>

                {/* the whole trace */}
                <div className="py-3 text-sm">
                  <span className="font-medium">All streams</span>
                  <p className="text-[0.7rem] text-muted-foreground">
                    {records.length} {records.length === 1 ? "step" : "steps"} · {streams}{" "}
                    {streams === 1 ? "stream" : "streams"}
                  </p>
                </div>
                <div className="relative h-full self-stretch" style={GRID}>
                  <span className="absolute top-[22px] left-0 h-4 w-full -translate-y-1/2 rounded-full bg-brand" />
                </div>
                <div className="self-stretch pt-3 text-right">
                  <div className="font-numeric text-xs font-medium">{fmtMs(span)}</div>
                  <div className="text-[0.65rem] text-muted-foreground">end to end</div>
                </div>

                {rows.map(({ record: r, depth, gap, wait, work, total, offset, color }, i) => (
                  <div key={`${r.stream}\u0000${r.message_id}\u0000${r.delivery}\u0000${i}`} className="contents">
                    <div className="flex min-w-0 flex-col gap-0.5 border-t pt-2.5 pb-2" style={{ paddingLeft: depth * 18 }}>
                      <span className="flex flex-wrap items-center gap-1.5 text-sm">
                        {depth > 0 && <span className="text-muted-foreground" aria-hidden>↳</span>}
                        <span className={cn("size-2 shrink-0 rounded-full", color)} aria-hidden />
                        <span className="font-medium">{r.stream}</span>
                        <span className="text-muted-foreground">/{r.group}</span>
                        <Badge variant={OUTCOME[r.outcome].variant} className="h-4 px-1.5 text-[0.65rem]">
                          {OUTCOME[r.outcome].label}
                        </Badge>
                      </span>
                      <span className="font-mono text-[0.68rem] text-muted-foreground">
                        msg {r.message_id}
                        {r.delivery > 1 ? ` · try ${r.delivery}` : ""}
                      </span>
                      <span className="font-mono text-[0.68rem] break-all text-muted-foreground">
                        worker {r.consumer} · node {r.node_id}
                      </span>
                      <span className="text-[0.68rem] text-muted-foreground">
                        {i === 0
                          ? "starts the trace"
                          : `published +${fmtMs(offset)}, ${gap > 0 ? `${fmtMs(gap)} after the previous step` : "while the previous step was still running"}`}
                      </span>
                    </div>
                    <div className="relative h-full min-h-14 self-stretch border-t" style={GRID}>
                      <span
                        className={cn("absolute top-5 h-1.5 -translate-y-1/2 rounded-full opacity-30", color)}
                        style={{ left: `${pct(r.published_at - from)}%`, width: `${pct(wait)}%`, minWidth: wait > 0 ? 2 : 0 }}
                      />
                      <span
                        className={cn("absolute top-5 h-4 -translate-y-1/2 rounded-full", color)}
                        style={{ left: `${pct(r.started_at - from)}%`, width: `${pct(work)}%`, minWidth: 2 }}
                        title={`${fmtMs(work)} in the handler`}
                      />
                    </div>
                    <div className="self-stretch border-t pt-2.5 text-right">
                      <div className="font-numeric text-xs font-medium">{fmtMs(total)}</div>
                      <div className="text-[0.65rem] text-muted-foreground">{fmtMs(work)} work</div>
                      {wait > 0 && (
                        <div className="text-[0.65rem] text-muted-foreground">{fmtMs(wait)} wait</div>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
          <p className="flex flex-wrap items-center gap-x-4 gap-y-1 text-[0.7rem] text-muted-foreground">
            <span className="flex items-center gap-1.5">
              <span className="h-1.5 w-4 rounded-full bg-foreground/25" aria-hidden />
              queue wait
            </span>
            <span className="flex items-center gap-1.5">
              <span className="h-2.5 w-4 rounded-full bg-foreground/70" aria-hidden />
              handler (colour per stream)
            </span>
            <span>metadata only · kept for one hour after the last attempt</span>
          </p>
        </>
      )}
    </section>
  );
}
