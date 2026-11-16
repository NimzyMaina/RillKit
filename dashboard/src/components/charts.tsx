import { useId, useMemo } from "react";
import { Area, Bar, BarChart, CartesianGrid, ComposedChart, Line, XAxis, YAxis } from "recharts";
import { ChartContainer, ChartLegend, ChartLegendContent, ChartTooltip, ChartTooltipContent, type ChartConfig } from "@/components/ui/chart";
import { fmtTime } from "@/lib/format";
import { cn } from "@/lib/utils";

// Every chart here has ONE y-axis (two measures of different scale get two charts), thin marks,
// a recessive grid, a crosshair tooltip, and colours from the validated --chart-* tokens.

type AxisEnd = number | "auto" | "dataMin" | "dataMax" | ((v: number) => number);

export interface Point {
  t: number;
  [key: string]: number | null;
}

function timeTicks(data: Point[]) {
  // short spans need seconds on the ticks, or neighbouring labels repeat the same minute
  const span = data.length > 1 ? data[data.length - 1].t - data[0].t : 0;
  return (v: number) => (span < 10 * 60_000 ? fmtTime(v) : fmtTime(v).slice(0, 5));
}

function xAxis(data: Point[], hide = false) {
  return (
    <XAxis
      dataKey="t"
      type="number"
      scale="time"
      domain={["dataMin", "dataMax"]}
      tickFormatter={timeTicks(data)}
      tickLine={false}
      axisLine={false}
      minTickGap={56}
      tickMargin={10}
      hide={hide}
      className="font-mono"
    />
  );
}

const tooltipLabel = (_: unknown, payload: readonly { payload?: unknown }[] | undefined) => {
  const t = (payload?.[0]?.payload as { t?: number } | undefined)?.t;
  return t ? fmtTime(t) : "";
};

/** One measure over time: an area, a line, or a step line (for counts that jump). */
export function SeriesChart({
  data,
  dataKey,
  label,
  color = "var(--chart-1)",
  kind = "area",
  format,
  domain,
  syncId,
  hideXAxis,
  axisWidth = 52,
  className,
}: {
  data: Point[];
  dataKey: string;
  label: string;
  color?: string;
  kind?: "area" | "line" | "step";
  format: (v: number) => string;
  /** Each end: a number, "auto", or a function of the data's min (or max). */
  domain?: [AxisEnd, AxisEnd];
  syncId?: string;
  hideXAxis?: boolean;
  /** Room for the y-axis labels; widen it for long units such as "26 KB/s". */
  axisWidth?: number;
  className?: string;
}) {
  const gradient = useId().replace(/:/g, "");
  const config = useMemo(() => ({ [dataKey]: { label, color } }) satisfies ChartConfig, [dataKey, label, color]);
  return (
    <ChartContainer config={config} className={cn("aspect-auto h-48 w-full", className)}>
      <ComposedChart data={data} syncId={syncId} margin={{ top: 8, right: 8, left: 4, bottom: 0 }}>
        <defs>
          <linearGradient id={gradient} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={`var(--color-${dataKey})`} stopOpacity={0.24} />
            <stop offset="100%" stopColor={`var(--color-${dataKey})`} stopOpacity={0.02} />
          </linearGradient>
        </defs>
        <CartesianGrid vertical={false} strokeDasharray="2 4" />
        {xAxis(data, hideXAxis)}
        <YAxis
          tickFormatter={format}
          tickLine={false}
          axisLine={false}
          width={axisWidth}
          allowDecimals={kind !== "step"}
          domain={domain ?? [0, "auto"]}
          className="font-mono"
        />
        <ChartTooltip
          cursor={{ strokeDasharray: "3 3" }}
          content={<ChartTooltipContent indicator="line" labelFormatter={tooltipLabel} formatter={(v) => (
            <span className="flex w-full justify-between gap-6">
              <span className="text-muted-foreground">{label}</span>
              <span className="font-mono font-numeric text-foreground">{format(Number(v))}</span>
            </span>
          )} />}
        />
        {kind === "area" ? (
          <Area dataKey={dataKey} type="monotone" stroke={`var(--color-${dataKey})`} strokeWidth={2} fill={`url(#${gradient})`} isAnimationActive={false} connectNulls />
        ) : (
          <Line dataKey={dataKey} type={kind === "step" ? "stepAfter" : "monotone"} stroke={`var(--color-${dataKey})`} strokeWidth={2} dot={false} isAnimationActive={false} connectNulls />
        )}
      </ComposedChart>
    </ChartContainer>
  );
}

/** Counts per time bucket, stacked (e.g. succeeded and failed), with a 2px gap between bars. */
export function StackedBars({
  data,
  series,
  format,
  className,
}: {
  data: Point[];
  series: { key: string; label: string; color: string }[];
  format: (v: number) => string;
  className?: string;
}) {
  const config = useMemo(
    () => Object.fromEntries(series.map((s) => [s.key, { label: s.label, color: s.color }])) satisfies ChartConfig,
    [series],
  );
  return (
    <ChartContainer config={config} className={cn("aspect-auto h-56 w-full", className)}>
      <BarChart data={data} barCategoryGap={2} margin={{ top: 8, right: 8, left: 4, bottom: 0 }}>
        <CartesianGrid vertical={false} strokeDasharray="2 4" />
        <XAxis
          dataKey="t"
          tickFormatter={timeTicks(data)}
          tickLine={false}
          axisLine={false}
          minTickGap={56}
          tickMargin={10}
          className="font-mono"
        />
        <YAxis tickFormatter={format} tickLine={false} axisLine={false} width={52} allowDecimals={false} className="font-mono" />
        <ChartTooltip cursor={{ fill: "var(--muted)", opacity: 0.5 }} content={<ChartTooltipContent labelFormatter={tooltipLabel} />} />
        <ChartLegend content={<ChartLegendContent />} />
        {series.map((s, i) => (
          <Bar
            key={s.key}
            dataKey={s.key}
            stackId="a"
            fill={`var(--color-${s.key})`}
            stroke="var(--card)"
            strokeWidth={1}
            radius={i === series.length - 1 ? [4, 4, 0, 0] : 0}
            isAnimationActive={false}
          />
        ))}
      </BarChart>
    </ChartContainer>
  );
}
