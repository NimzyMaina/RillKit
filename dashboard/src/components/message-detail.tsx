import { useEffect, useState, type ReactNode } from "react";
import { EyeOffIcon, ScrollTextIcon, XIcon } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { LogList } from "@/components/log-list";
import { TraceView } from "@/components/trace-view";
import { errorMessage, errorTitle, useMessageLogs } from "@/api/queries";
import type { MessageStatus, StreamMessage } from "@/api/types";
import { fmtBytes, fmtDateTime, idTime, prettyData } from "@/lib/format";

export function MessageStatusBadge({ m }: { m: Pick<StreamMessage, "status" | "deliveries"> }) {
  const status: MessageStatus | undefined = m.status;
  if (!status) return <span className="text-muted-foreground">—</span>;
  if (status === "acked") return <Badge variant="running">Acked</Badge>;
  if (status === "waiting") return <Badge variant="outline">Waiting</Badge>;
  return (
    <Badge variant="paused">
      Pending{m.deliveries && m.deliveries > 1 ? ` · try ${m.deliveries}` : ""}
    </Badge>
  );
}

/** Explains a hidden part of a message and the worker setting that shows it. */
export function PrivacyNote({ what, variable, children }: { what: string; variable: string; children?: ReactNode }) {
  return (
    <div className="flex gap-3 rounded-2xl border border-dashed bg-muted/30 p-4 text-sm">
      <EyeOffIcon className="mt-0.5 size-4 shrink-0 text-muted-foreground" aria-hidden />
      <div className="flex flex-col gap-1.5">
        <p className="font-medium">{what}</p>
        <p className="text-muted-foreground">
          {children ?? "The workers keep it private."} Set{" "}
          <code className="rounded-md bg-muted px-1.5 py-0.5 font-mono text-[0.75rem] text-foreground">{variable}=true</code>{" "}
          on the workers that consume this stream to show it here.
        </p>
      </div>
    </div>
  );
}

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="flex flex-col gap-3">
      <h3 className="text-xs font-medium tracking-wide text-muted-foreground uppercase">{title}</h3>
      {children}
    </section>
  );
}

export function Payload({ m }: { m: StreamMessage }) {
  if (!m.fields) return null;
  const { data, traceparent: _t, ...headers } = m.fields;
  void _t;
  const pretty = data !== undefined ? prettyData(data) : null;
  return (
    <div className="flex flex-col gap-3">
      {pretty ? (
        <pre className="max-h-80 overflow-auto rounded-2xl bg-muted p-4 font-mono text-xs leading-relaxed whitespace-pre-wrap break-all">
          {pretty.text}
        </pre>
      ) : (
        <p className="text-sm text-muted-foreground">This entry has no data field.</p>
      )}
      {Object.keys(headers).length > 0 && (
        <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 rounded-2xl border p-3 font-mono text-xs">
          {Object.entries(headers).map(([k, v]) => (
            <div key={k} className="contents">
              <dt className="text-muted-foreground">{k}</dt>
              <dd className="break-all">{v}</dd>
            </div>
          ))}
        </dl>
      )}
    </div>
  );
}

interface Props {
  tenant: string;
  stream: string;
  message: StreamMessage | null;
  loading?: boolean;
  error?: unknown;
  payloads: boolean;
  captureLogs: boolean;
  /** RILLKIT_TRACE_RECORDS on the workers: whether a trace waterfall can be read. */
  traceRecords: boolean;
  onClose: () => void;
  /** Extra sections or actions (the DLQ's replay and edit controls). */
  children?: ReactNode;
}

/**
 * One message, expanded in place under its row: state, payload, trace waterfall and logs. No
 * overlay: the list stays where it was, and closing returns the row to its normal height.
 */
export function MessageDetail({
  tenant,
  stream,
  message: m,
  loading,
  error,
  payloads,
  captureLogs,
  traceRecords,
  onClose,
  children,
}: Props) {
  const dead = m?.dead_letter;
  // a DLQ entry's logs were written under the id it had in the stream
  const logId = dead?.original_id ?? m?.id ?? null;
  const logs = useMessageLogs(tenant, stream, logId, Boolean(m && captureLogs));
  const traceId = m?.traceparent ? m.traceparent.split("-")[1] : null;
  const [showTrace, setShowTrace] = useState(false);

  useEffect(() => {
    setShowTrace(false);
  }, [m?.id]);

  return (
    <div
      data-testid="message-detail"
      aria-label={m ? `Message ${m.id}` : "Message"}
      className="flex flex-col gap-6 border-t-2 border-brand/25 bg-muted/20 px-5 py-5"
    >
      <div className="flex flex-wrap items-start justify-between gap-3">
        {m ? (
          <div className="flex min-w-0 flex-col gap-1">
            <span className="flex flex-wrap items-center gap-2">
              {dead ? <Badge variant="draining">Dead-lettered</Badge> : <MessageStatusBadge m={m} />}
              <span className="font-mono text-xs">{m.id}</span>
            </span>
            <p className="flex flex-wrap items-baseline gap-x-3 gap-y-1 text-xs text-muted-foreground">
              <span className="font-numeric">{fmtDateTime(idTime(m.id))}</span>
              <span className="font-numeric">{fmtBytes(m.size)}</span>
              {m.consumer && (
                <span>
                  consumer <span className="font-mono text-foreground">{m.consumer}</span>
                </span>
              )}
              {dead && (
                <>
                  <span>
                    original <span className="font-mono text-foreground">{dead.original_id}</span>
                  </span>
                  <span className="font-numeric">
                    {dead.deliveries} {dead.deliveries === 1 ? "delivery" : "deliveries"}
                  </span>
                </>
              )}
            </p>
          </div>
        ) : (
          <div className="flex flex-col gap-2">
            <Skeleton className="h-5 w-32 rounded-md" />
            <Skeleton className="h-4 w-64 rounded-md" />
          </div>
        )}
        <Button variant="ghost" size="icon-sm" aria-label="Close message" data-action="close-message" onClick={onClose}>
          <XIcon />
        </Button>
      </div>

      {loading && !m ? (
        <div className="flex flex-col gap-3">
          <Skeleton className="h-10 rounded-2xl" />
          <Skeleton className="h-40 rounded-2xl" />
        </div>
      ) : error && !m ? (
        <Alert variant="destructive">
          <AlertTitle>{errorTitle(error)}</AlertTitle>
          <AlertDescription>{errorMessage(error)}</AlertDescription>
        </Alert>
      ) : m ? (
        <>
          {dead && (
            <Section title="Why it failed">
              {dead.error !== null ? (
                <p className="rounded-2xl bg-state-draining-fill/60 px-4 py-3 font-mono text-[0.8rem] break-words text-foreground">
                  {dead.error}
                </p>
              ) : (
                <PrivacyNote what="The error text is hidden" variable="RILLKIT_INSPECT_MESSAGES">
                  Error texts often quote the message, so the workers keep them private.
                </PrivacyNote>
              )}
              <dl className="grid grid-cols-[auto_1fr] gap-x-6 gap-y-1.5 text-sm">
                <dt className="text-muted-foreground">Stream</dt>
                <dd className="font-mono text-xs leading-5">{dead.stream}</dd>
                <dt className="text-muted-foreground">Group</dt>
                <dd className="font-mono text-xs leading-5">{dead.group}</dd>
              </dl>
            </Section>
          )}

          {children}

          <Section title="Payload">
            {payloads && m.fields ? (
              <Payload m={m} />
            ) : (
              <PrivacyNote what="The payload is hidden" variable="RILLKIT_INSPECT_MESSAGES" />
            )}
          </Section>

          <Section title="Trace">
            {m.traceparent ? (
              <div className="flex flex-col gap-3">
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <p className="font-mono text-xs break-all text-muted-foreground">{m.traceparent}</p>
                  {traceRecords && traceId ? (
                    <Button
                      size="sm"
                      variant="outline"
                      data-action="view-trace"
                      aria-expanded={showTrace}
                      onClick={() => setShowTrace((v) => !v)}
                    >
                      {showTrace ? "Hide trace" : "View trace"}
                    </Button>
                  ) : null}
                </div>
                {!traceRecords && (
                  <PrivacyNote what="No trace waterfall" variable="RILLKIT_TRACE_RECORDS">
                    The workers keep no per-trace records, so only this message's id shows. Attempt
                    timings are still in the stream's Time per message chart.
                  </PrivacyNote>
                )}
                {showTrace && traceId && <TraceView tenant={tenant} traceId={traceId} />}
              </div>
            ) : (
              <p className="text-sm text-muted-foreground">
                No traceparent: the producer does not propagate trace context.
              </p>
            )}
          </Section>

          <Section title={dead ? "What happened" : "Logs"}>
            {!captureLogs ? (
              <PrivacyNote what="Logs are not captured" variable="RILLKIT_CAPTURE_LOGS">
                The workers do not store what their handlers log.
              </PrivacyNote>
            ) : logs.isPending ? (
              <div className="flex flex-col gap-3">
                {Array.from({ length: 2 }, (_, i) => (
                  <Skeleton key={i} className="h-16 rounded-2xl" />
                ))}
              </div>
            ) : logs.error ? (
              <Alert variant="destructive">
                <ScrollTextIcon />
                <AlertTitle>{errorTitle(logs.error)}</AlertTitle>
                <AlertDescription>{errorMessage(logs.error)}</AlertDescription>
              </Alert>
            ) : (
              <LogList records={logs.data?.records ?? []} />
            )}
          </Section>
        </>
      ) : null}
    </div>
  );
}
