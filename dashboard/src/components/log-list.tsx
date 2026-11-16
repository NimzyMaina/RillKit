import { ScrollTextIcon } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import type { LogRecord } from "@/api/types";
import { fmtTime } from "@/lib/format";
import { cn } from "@/lib/utils";

const LEVEL_VARIANT: Record<string, "outline" | "lavender" | "paused" | "draining"> = {
  DEBUG: "outline",
  INFO: "lavender",
  WARNING: "paused",
  WARN: "paused",
  ERROR: "draining",
  CRITICAL: "draining",
};

/** What a handler logged for one message, oldest first, grouped visually by delivery. */
export function LogList({ records }: { records: LogRecord[] }) {
  if (records.length === 0) {
    return (
      <Empty className="py-8">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <ScrollTextIcon />
          </EmptyMedia>
          <EmptyTitle>No logs for this message</EmptyTitle>
          <EmptyDescription>It has not been handled yet, its logs expired, or its handler logged nothing.</EmptyDescription>
        </EmptyHeader>
      </Empty>
    );
  }
  return (
    <ol className="flex flex-col gap-3" aria-label="Log records">
      {records.map((r, i) => (
        <LogItem key={i} r={r} />
      ))}
    </ol>
  );
}

function LogItem({ r }: { r: LogRecord }) {
  return (
    <li className="flex flex-col gap-2 rounded-2xl border bg-card p-4">
      <div className="flex flex-wrap items-center gap-2">
        <Badge variant={LEVEL_VARIANT[r.level.toUpperCase()] ?? "outline"} className="font-mono text-[0.7rem]">
          {r.level}
        </Badge>
        <span className="font-mono text-xs text-muted-foreground">{r.logger}</span>
        {r.delivery && <span className="font-mono text-xs text-muted-foreground">· delivery {r.delivery}</span>}
        <span className="ml-auto font-mono text-xs text-muted-foreground font-numeric">{fmtTime(r.ts)}</span>
      </div>
      <p className="text-sm break-words">{r.message}</p>
      {(r.exc_type || r.exc) && (
        <div className="flex flex-col gap-1.5">
          {r.exc_type && (
            <span className={cn("w-fit rounded-md bg-state-draining-fill px-2 py-0.5 font-mono text-xs text-state-draining")}>
              {r.exc_type}
            </span>
          )}
          {r.exc && (
            <pre className="max-h-56 overflow-auto rounded-xl bg-muted p-3 font-mono text-[0.7rem] leading-relaxed whitespace-pre-wrap">
              {r.exc}
            </pre>
          )}
        </div>
      )}
      {(r.consumer || r.traceparent) && (
        <div className="flex flex-wrap gap-x-4 gap-y-1 font-mono text-[0.7rem] text-muted-foreground">
          {r.consumer && <span>consumer {r.consumer}</span>}
          {r.traceparent && <span className="truncate">trace {r.traceparent}</span>}
        </div>
      )}
    </li>
  );
}
