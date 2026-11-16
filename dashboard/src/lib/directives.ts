import type { Directive, DirectiveInput, WorkerState } from "@/api/types";

const STATE_VERB: Record<WorkerState, string> = {
  RUNNING: "Resume",
  PAUSED: "Pause",
  DRAINING: "Drain",
  MAINTENANCE: "Maintenance",
};

export function targetLabel(d: Pick<Directive, "scope" | "target">): string {
  if (d.scope === "global") return "all workers";
  if (d.scope === "node") return d.target.node_id ?? "node";
  return `${d.target.stream ?? "?"} / ${d.target.group ?? "?"}`;
}

/** A short human description, e.g. "Pause orders / billing". */
export function describe(d: Pick<Directive, "cmd" | "scope" | "target" | "params">): string {
  const where = targetLabel(d);
  const p = d.params ?? {};
  switch (d.cmd) {
    case "SET_STATE":
      return `${STATE_VERB[p.state as WorkerState] ?? "Set state of"} ${where}`;
    case "MAINTENANCE_ON":
      return `Maintenance on: ${where}`;
    case "MAINTENANCE_OFF":
      return `Maintenance off: ${where}`;
    case "SET_CONCURRENCY":
      return `${where}: ${String(p.concurrency)} workers${d.scope === "stream" ? " per node" : ""}`;
    case "SET_POLL_CONFIG":
      return `Poll settings for ${where}`;
    case "EXECUTE_REPLAY": {
      if (Array.isArray(p.ids)) {
        const n = p.ids.length;
        const edited = p.data && typeof p.data === "object" ? " (edited)" : "";
        return `Replay ${n} DLQ ${n === 1 ? "entry" : "entries"}${edited} of ${where}`;
      }
      if (p.set_id !== undefined) return `Move ${where} to ${String(p.set_id)}`;
      return `Replay ${String(p.count ?? 100)} from the DLQ of ${where}`;
    }
    case "DELETE_DLQ": {
      if (p.all === true) return `Purge the DLQ of ${where}`;
      const n = Array.isArray(p.ids) ? p.ids.length : 0;
      return `Delete ${n} DLQ ${n === 1 ? "entry" : "entries"} of ${where}`;
    }
    case "CREATE_STREAM":
      return `Create ${where}${p.start === "0" ? " from the start" : ""}`;
  }
}

/** The ack's result in words, e.g. "replayed 2, 1 missing"; null when there is none. */
export function resultSummary(d: Pick<Directive, "ack">): string | null {
  const r = d.ack?.result;
  if (!r) return null;
  const parts: string[] = [];
  if (typeof r.replayed === "number") parts.push(`replayed ${r.replayed}`);
  if (Array.isArray(r.missing) && r.missing.length > 0) parts.push(`${r.missing.length} missing`);
  if (typeof r.deleted === "number") parts.push(`deleted ${r.deleted}`);
  if (typeof r.created === "boolean") parts.push(r.created ? "created" : "group already existed");
  if (typeof r.set_id === "string") parts.push(`moved to ${r.set_id}`);
  return parts.length ? parts.join(", ") : null;
}

export function inputLabel(input: DirectiveInput): string {
  return describe({ ...input, target: input.target ?? {}, params: input.params ?? {} });
}
