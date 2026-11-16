import { useId, useMemo } from "react";
import { SeriesChart, type Point } from "@/components/charts";
import type { Sample } from "@/api/types";
import { fmtCompact, fmtInt } from "@/lib/format";

/**
 * Lag and workers over time, as two aligned charts with one axis each (never a dual axis),
 * sharing the time axis and the hover crosshair.
 */
export function LagChart({ samples }: { samples: Sample[] }) {
  const sync = useId();
  const data = useMemo<Point[]>(
    () => samples.map((s) => ({ t: Date.parse(s.ts), lag: s.lag, workers: s.concurrency })),
    [samples],
  );
  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-baseline justify-between text-xs text-muted-foreground">
        <span className="font-medium text-foreground">Lag</span>
        <span>messages waiting in the group</span>
      </div>
      <SeriesChart data={data} dataKey="lag" label="Lag" format={fmtCompact} syncId={sync} hideXAxis className="h-52 md:h-60" />
      <div className="flex items-baseline justify-between pt-2 text-xs text-muted-foreground">
        <span className="font-medium text-foreground">Workers</span>
        <span>lanes across all nodes</span>
      </div>
      <SeriesChart
        data={data}
        dataKey="workers"
        label="Workers"
        kind="step"
        format={fmtInt}
        syncId={sync}
        domain={[0, (max: number) => Math.max(4, Math.ceil(max * 1.2))]}
        className="h-32"
      />
    </div>
  );
}
