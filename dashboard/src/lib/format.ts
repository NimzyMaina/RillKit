const nf = new Intl.NumberFormat("en-US");
const compact = new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 });

export function fmtInt(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  return nf.format(Math.round(v));
}

export function fmtCompact(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  return Math.abs(v) < 10000 ? nf.format(Math.round(v)) : compact.format(v);
}

export function fmtRate(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  return v >= 100 ? nf.format(Math.round(v)) : v.toFixed(1);
}

export function fmtPct(v: number | null | undefined): string {
  if (v === null || v === undefined) return "—";
  return `${v.toFixed(0)}%`;
}

export function fmtMb(v: number | null | undefined): string {
  if (v === null || v === undefined) return "—";
  return v >= 1024 ? `${(v / 1024).toFixed(1)} GB` : `${Math.round(v)} MB`;
}

/** A duration in milliseconds: below 1 s in ms, above it in s (and above a minute, in min). */
export function fmtMs(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  if (v < 1) return "<1 ms";
  if (v < 1000) return `${Math.round(v)} ms`;
  if (v < 60000) return `${(v / 1000).toFixed(v < 10000 ? 1 : 0)} s`;
  return `${(v / 60000).toFixed(1)} min`;
}

const rtf = new Intl.RelativeTimeFormat("en", { numeric: "auto", style: "short" });

export function relativeTime(isoTs: string | null | undefined, now = Date.now()): string {
  if (!isoTs) return "—";
  const diff = (Date.parse(isoTs) - now) / 1000;
  const abs = Math.abs(diff);
  if (abs < 5) return "just now";
  if (abs < 60) return rtf.format(Math.round(diff), "second");
  if (abs < 3600) return rtf.format(Math.round(diff / 60), "minute");
  if (abs < 86400) return rtf.format(Math.round(diff / 3600), "hour");
  return rtf.format(Math.round(diff / 86400), "day");
}

const dtf = new Intl.DateTimeFormat("en-GB", {
  day: "2-digit",
  month: "short",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});

/**
 * A timestamp from the API as a Date: an ISO 8601 string, epoch milliseconds, or epoch
 * milliseconds as a string of digits (how workers record log times). Null if it is not a time.
 */
export function toDate(ts: string | number | null | undefined): Date | null {
  if (ts === null || ts === undefined || ts === "") return null;
  const d = typeof ts === "string" && /^\d+$/.test(ts) ? new Date(Number(ts)) : new Date(ts);
  return Number.isNaN(d.getTime()) ? null : d;
}

// Formatting never throws: a bad timestamp shows as a dash instead of breaking the page.
export function fmtDateTime(ts: string | number | null | undefined): string {
  const d = toDate(ts);
  return d ? dtf.format(d) : "—";
}

const tf = new Intl.DateTimeFormat("en-GB", { hour: "2-digit", minute: "2-digit", second: "2-digit" });

export function fmtTime(ts: string | number | null | undefined): string {
  const d = toDate(ts);
  return d ? tf.format(d) : "—";
}

/** A size in bytes: 512 B, 3.4 KB, 12.8 MB. */
export function fmtBytes(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  if (v < 1024) return `${Math.round(v)} B`;
  if (v < 1024 ** 2) return `${(v / 1024).toFixed(v < 10 * 1024 ? 1 : 0)} KB`;
  if (v < 1024 ** 3) return `${(v / 1024 ** 2).toFixed(1)} MB`;
  return `${(v / 1024 ** 3).toFixed(1)} GB`;
}

/** The time of a stream entry, from its id: "1794765340386-0" is epoch milliseconds, then a seq. */
export function idTime(id: string): number | null {
  const ms = Number(id.split("-")[0]);
  return Number.isFinite(ms) && ms > 0 ? ms : null;
}

/** The entry's `data` as pretty JSON when it is JSON, else as it is. */
export function prettyData(data: string): { text: string; json: boolean } {
  try {
    return { text: JSON.stringify(JSON.parse(data), null, 2), json: true };
  } catch {
    return { text: data, json: false };
  }
}
