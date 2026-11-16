import { useId, useMemo } from "react";
import { SeriesChart, type Point } from "@/components/charts";
import type { Sample } from "@/api/types";
import { fmtMs } from "@/lib/format";

/**
 * Queue wait and handler time per attempt, as two aligned charts with one axis each (never a
 * dual axis), sharing the time axis and the hover crosshair.
 */
export function TimeChart({ samples }: { samples: Sample[] }) {
  const sync = useId();
  const data = useMemo<Point[]>(
    () => samples.map((s) => ({ t: Date.parse(s.ts), wait: s.wait_ms, work: s.work_ms })),
    [samples],
  );
  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-baseline justify-between text-xs text-muted-foreground">
        <span className="font-medium text-foreground">Queue wait</span>
        <span>from publish to handler start</span>
      </div>
      <SeriesChart data={data} dataKey="wait" label="Wait" format={fmtMs} syncId={sync} hideXAxis className="h-44 md:h-52" />
      <div className="flex items-baseline justify-between pt-2 text-xs text-muted-foreground">
        <span className="font-medium text-foreground">Work</span>
        <span>time spent in the handler</span>
      </div>
      <SeriesChart data={data} dataKey="work" label="Work" format={fmtMs} syncId={sync} className="h-36 md:h-44" />
    </div>
  );
}
