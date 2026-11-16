import { useQueryClient } from "@tanstack/react-query";
import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import { api, appendSamples, isTerminal, qk, reportDirective, upsertDirective } from "./queries";
import type { DirectiveCmd } from "./types";

/** Directives that change a stream's entries: its message lists are stale once they finish. */
const CHANGES_ENTRIES: DirectiveCmd[] = ["EXECUTE_REPLAY", "DELETE_DLQ", "CREATE_STREAM"];

export type LiveStatus = "connecting" | "live" | "reconnecting";

const LiveContext = createContext<LiveStatus>("connecting");

/**
 * Opens the tenant's SSE stream and pushes `overview` and `directive` events into the query
 * cache, so screens update without polling. After a reconnect everything is refetched, since
 * events may have been missed while the stream was down.
 */
export function LiveProvider({ tenant, children }: { tenant: string; children: ReactNode }) {
  const qc = useQueryClient();
  const [status, setStatus] = useState<LiveStatus>("connecting");

  useEffect(() => {
    setStatus("connecting");
    const close = api().subscribe(tenant, {
      onOverview: (ov) => {
        qc.setQueryData(qk.overview(tenant), ov);
        appendSamples(qc, tenant, ov);
      },
      onDirective: (d) => {
        upsertDirective(qc, tenant, d);
        reportDirective(d);
        if (isTerminal(d.status) && CHANGES_ENTRIES.includes(d.cmd) && d.target.stream && d.target.group) {
          void qc.invalidateQueries({ queryKey: qk.messagesFor(tenant, d.target.stream, d.target.group) });
          void qc.invalidateQueries({ queryKey: ["message", tenant, d.target.stream, d.target.group] });
          void qc.invalidateQueries({ queryKey: ["metrics", tenant] });
        }
      },
      onOpen: (reconnect) => {
        setStatus("live");
        if (reconnect) {
          void qc.invalidateQueries({ queryKey: qk.overview(tenant) });
          void qc.invalidateQueries({ queryKey: qk.directives(tenant) });
          void qc.invalidateQueries({ queryKey: qk.policies(tenant) });
          void qc.invalidateQueries({ queryKey: ["samples", tenant] });
          void qc.invalidateQueries({ queryKey: ["messages", tenant] });
          void qc.invalidateQueries({ queryKey: ["metrics", tenant] });
        }
      },
      onError: () => setStatus("reconnecting"),
    });
    return close;
  }, [qc, tenant]);

  return <LiveContext.Provider value={status}>{children}</LiveContext.Provider>;
}

export function useLiveStatus(): LiveStatus {
  return useContext(LiveContext);
}
