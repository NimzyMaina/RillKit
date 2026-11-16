import { Fragment, useEffect, useMemo, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { ChevronDownIcon, ChevronLeftIcon, ChevronRightIcon, PencilIcon, RotateCcwIcon, ShieldCheckIcon, Trash2Icon } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardAction, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Textarea } from "@/components/ui/textarea";
import { Confirm } from "@/components/confirm";
import { MessageDetail } from "@/components/message-detail";
import { DirectiveStatusBadge } from "@/components/state-badge";
import { errorMessage, errorTitle, isTerminal, qk, useDirective, useMessages, useSendDirective } from "@/api/queries";
import type { DirectiveInput, StreamMessage, StreamSummary } from "@/api/types";
import { inputLabel, resultSummary } from "@/lib/directives";
import { fmtBytes, fmtDateTime, fmtInt, idTime, prettyData } from "@/lib/format";

const PAGE = 25;

/** Sends DLQ directives, follows the last one, and refreshes the DLQ once the worker acks it. */
function useDlqDirective(tenant: string, s: StreamSummary, onDone: () => void) {
  const qc = useQueryClient();
  const send = useSendDirective(tenant);
  const [lastId, setLastId] = useState<string | null>(null);
  const last = useDirective(tenant, lastId);
  const terminal = last ? isTerminal(last.status) : false;
  const onDoneRef = useRef(onDone);
  onDoneRef.current = onDone;
  const handled = useRef<string | null>(null);
  useEffect(() => {
    // once per finished directive: refresh the DLQ (and the stream's messages) and reset
    if (!terminal || !lastId || handled.current === lastId) return;
    handled.current = lastId;
    void qc.invalidateQueries({ queryKey: qk.messagesFor(tenant, s.stream, s.group) });
    onDoneRef.current();
  }, [terminal, lastId, qc, tenant, s.stream, s.group]);
  const run = (params: Record<string, unknown>, cmd: DirectiveInput["cmd"] = "EXECUTE_REPLAY") => {
    const input: DirectiveInput = { cmd, scope: "stream", target: { stream: s.stream, group: s.group }, params };
    send.mutate({ input, label: inputLabel(input) }, { onSuccess: (d) => setLastId(d.id) });
  };
  return { run, busy: send.isPending || Boolean(last && !terminal), last };
}

function shortError(m: StreamMessage): string {
  const e = m.dead_letter?.error;
  if (e === null || e === undefined) return "";
  return e.length > 90 ? `${e.slice(0, 90)}…` : e;
}

export function DlqTab({ tenant, s }: { tenant: string; s: StreamSummary }) {
  const [cursors, setCursors] = useState<(string | null)[]>([null]);
  const before = cursors[cursors.length - 1];
  const page = useMessages(tenant, s.stream, s.group, { source: "dlq", before, count: PAGE }, { enabled: s.nodes > 0 });
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [open, setOpen] = useState<StreamMessage | null>(null);
  const { run, busy, last } = useDlqDirective(tenant, s, () => {
    setSelected(new Set());
    setOpen(null);
  });

  const messages = page.data?.messages ?? [];
  const payloads = page.data?.payloads ?? s.capabilities.inspect_messages;
  const total = page.data?.total ?? s.dlq_length ?? 0;
  const pageIds = messages.map((m) => m.id);
  const allOnPage = pageIds.length > 0 && pageIds.every((id) => selected.has(id));
  const someOnPage = pageIds.some((id) => selected.has(id));

  const toggle = (id: string, on: boolean) =>
    setSelected((prev) => {
      const next = new Set(prev);
      if (on) next.add(id);
      else next.delete(id);
      return next;
    });

  if (s.nodes === 0) {
    return (
      <Card>
        <CardContent>
          <Empty className="py-12">
            <EmptyHeader>
              <EmptyTitle>No worker consumes this stream</EmptyTitle>
              <EmptyDescription>The DLQ is read through a connected worker. Start one for this stream to see it.</EmptyDescription>
            </EmptyHeader>
          </Empty>
        </CardContent>
      </Card>
    );
  }

  const ids = [...selected];
  return (
    <Card>
      <CardHeader>
        <CardTitle>Dead-letter queue</CardTitle>
        <CardDescription>
          Messages that failed every retry, newest first, in <span className="font-mono">{s.dlq_key}</span>. Replaying one
          adds it back to the stream, where your handlers process it again.
        </CardDescription>
        <CardAction className="flex flex-wrap items-center justify-end gap-2">
          <Confirm
            title={`Replay all ${fmtInt(total)} messages?`}
            description={<p>Every entry in the DLQ goes back onto the stream, oldest first, and is handled again.</p>}
            confirmLabel="Replay all"
            onConfirm={() => run({ count: Math.max(1, Math.min(total, 100000)) })}
            trigger={
              <Button variant="secondary" size="sm" disabled={busy || total === 0}>
                <RotateCcwIcon data-icon="inline-start" />
                Replay all
              </Button>
            }
          />
          <Confirm
            destructive
            title="Purge the DLQ?"
            description={
              <p>
                All {fmtInt(total)} entries are deleted for good. Their messages will not be handled again.
              </p>
            }
            confirmLabel="Purge DLQ"
            onConfirm={() => run({ all: true }, "DELETE_DLQ")}
            trigger={
              <Button variant="ghost" size="sm" className="text-destructive" disabled={busy || total === 0}>
                <Trash2Icon data-icon="inline-start" />
                Purge
              </Button>
            }
          />
        </CardAction>
      </CardHeader>

      <CardContent className="flex flex-col gap-4">
        {selected.size > 0 && (
          <div className="flex flex-wrap items-center gap-3 rounded-2xl bg-lavender/40 px-4 py-2.5 text-sm dark:bg-lavender/10" role="region" aria-label="Selection">
            <span className="font-medium font-numeric">{fmtInt(selected.size)} selected</span>
            <Button size="sm" disabled={busy} onClick={() => run({ ids })}>
              <RotateCcwIcon data-icon="inline-start" />
              Replay selected
            </Button>
            <Confirm
              destructive
              title={`Delete ${fmtInt(selected.size)} ${selected.size === 1 ? "entry" : "entries"}?`}
              description={<p>They are removed from the DLQ for good and will not be handled again.</p>}
              confirmLabel="Delete"
              onConfirm={() => run({ ids }, "DELETE_DLQ")}
              trigger={
                <Button size="sm" variant="ghost" className="text-destructive" disabled={busy}>
                  <Trash2Icon data-icon="inline-start" />
                  Delete selected
                </Button>
              }
            />
            <Button size="sm" variant="ghost" className="ml-auto" onClick={() => setSelected(new Set())}>
              Clear
            </Button>
          </div>
        )}

        {last && (
          <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground" aria-live="polite">
            <span>Last change</span>
            <DirectiveStatusBadge status={last.status} />
            <span>{inputLabel({ cmd: last.cmd, scope: last.scope, target: last.target, params: last.params })}</span>
            {resultSummary(last) && <span className="font-mono text-foreground">{resultSummary(last)}</span>}
            {last.ack?.error && <span className="text-destructive">{last.ack.error}</span>}
          </div>
        )}

        {page.error ? (
          <Alert variant="destructive">
            <AlertTitle>{errorTitle(page.error)}</AlertTitle>
            <AlertDescription>{errorMessage(page.error)}</AlertDescription>
          </Alert>
        ) : page.isPending ? (
          <div className="flex flex-col gap-2">
            {Array.from({ length: 6 }, (_, i) => (
              <Skeleton key={i} className="h-10 rounded-xl" />
            ))}
          </div>
        ) : messages.length === 0 ? (
          <Empty className="py-12">
            <EmptyHeader>
              <EmptyMedia variant="icon">
                <ShieldCheckIcon />
              </EmptyMedia>
              <EmptyTitle>The DLQ is empty</EmptyTitle>
              <EmptyDescription>Nothing has failed all its retries. Messages land here when they do.</EmptyDescription>
            </EmptyHeader>
          </Empty>
        ) : (
          <div className="overflow-x-auto rounded-2xl border">
            <Table data-testid="dlq-table">
              <TableHeader>
                <TableRow>
                  <TableHead className="w-10">
                    <Checkbox
                      aria-label="Select this page"
                      checked={allOnPage ? true : someOnPage ? "indeterminate" : false}
                      onCheckedChange={(v) => pageIds.forEach((id) => toggle(id, v === true))}
                    />
                  </TableHead>
                  <TableHead>Failed at</TableHead>
                  <TableHead>Original message</TableHead>
                  <TableHead className="text-right">Tries</TableHead>
                  <TableHead className="w-full">Reason</TableHead>
                  <TableHead className="text-right">Size</TableHead>
                  <TableHead className="w-8" />
                </TableRow>
              </TableHeader>
              <TableBody>
                {messages.map((m) => {
                  const expanded = open?.id === m.id;
                  return (
                    <Fragment key={m.id}>
                      <TableRow
                        data-state={selected.has(m.id) ? "selected" : undefined}
                        data-dlq-row
                        aria-expanded={expanded}
                        className="cursor-pointer"
                        onClick={() => setOpen(expanded ? null : m)}
                        onKeyDown={(e) => {
                          if (e.key === "Enter" || e.key === " ") {
                            e.preventDefault();
                            setOpen(expanded ? null : m);
                          }
                        }}
                        tabIndex={0}
                      >
                        <TableCell onClick={(e) => e.stopPropagation()}>
                          <Checkbox
                            aria-label={`Select ${m.id}`}
                            checked={selected.has(m.id)}
                            onCheckedChange={(v) => toggle(m.id, v === true)}
                          />
                        </TableCell>
                        <TableCell className="font-mono text-xs whitespace-nowrap text-muted-foreground font-numeric">
                          {fmtDateTime(idTime(m.id))}
                        </TableCell>
                        <TableCell className="font-mono text-xs">{m.dead_letter?.original_id ?? "—"}</TableCell>
                        <TableCell className="text-right font-numeric">{m.dead_letter?.deliveries ?? "—"}</TableCell>
                        <TableCell className="max-w-0 truncate font-mono text-xs">
                          {payloads ? shortError(m) : <span className="text-muted-foreground italic">hidden</span>}
                        </TableCell>
                        <TableCell className="text-right font-mono text-xs font-numeric">{fmtBytes(m.size)}</TableCell>
                        <TableCell className="text-right">
                          <ChevronDownIcon
                            className={expanded ? "ml-auto size-4 rotate-180 text-muted-foreground transition-transform" : "ml-auto size-4 text-muted-foreground transition-transform"}
                            aria-hidden
                          />
                        </TableCell>
                      </TableRow>
                      {expanded && (
                        <TableRow className="hover:bg-transparent" data-detail-row>
                          <TableCell colSpan={7} className="p-0">
                            <MessageDetail
                              key={m.id}
                              tenant={tenant}
                              stream={s.stream}
                              message={m}
                              payloads={payloads}
                              captureLogs={s.capabilities.capture_logs}
                              traceRecords={s.capabilities.trace_records}
                              onClose={() => setOpen(null)}
                            >
                              <EntryActions key={m.id} m={m} payloads={payloads} busy={busy} run={run} />
                            </MessageDetail>
                          </TableCell>
                        </TableRow>
                      )}
                    </Fragment>
                  );
                })}
              </TableBody>
            </Table>
          </div>
        )}
      </CardContent>

      <CardFooter className="justify-between gap-4 text-xs text-muted-foreground">
        <span className="font-numeric" data-testid="dlq-page">
          Page {cursors.length} · {fmtInt(total)} {total === 1 ? "entry" : "entries"}
        </span>
        <div className="flex gap-2">
          <Button
            variant="outline"
            size="sm"
            disabled={cursors.length === 1}
            onClick={() => setCursors((c) => c.slice(0, -1))}
          >
            <ChevronLeftIcon data-icon="inline-start" />
            Previous
          </Button>
          <Button
            variant="outline"
            size="sm"
            disabled={!page.data?.next_before}
            onClick={() => page.data?.next_before && setCursors((c) => [...c, page.data!.next_before])}
          >
            Next
            <ChevronRightIcon data-icon="inline-end" />
          </Button>
        </div>
      </CardFooter>
    </Card>
  );
}

function EntryActions({
  m,
  payloads,
  busy,
  run,
}: {
  m: StreamMessage;
  payloads: boolean;
  busy: boolean;
  run: (params: Record<string, unknown>, cmd?: DirectiveInput["cmd"]) => void;
}) {
  const original = m.fields?.data;
  const initial = useMemo(() => (original !== undefined ? prettyData(original).text : ""), [original]);
  const [editing, setEditing] = useState(false);
  const [text, setText] = useState(initial);
  const parsed = useMemo(() => {
    try {
      return { ok: true as const, value: JSON.parse(text) as unknown };
    } catch (e) {
      return { ok: false as const, error: e instanceof Error ? e.message : String(e) };
    }
  }, [text]);
  const changed = text !== initial;

  return (
    <section className="flex flex-col gap-3 rounded-2xl border bg-muted/30 p-4">
      <div className="flex flex-wrap items-center gap-2">
        <Button size="sm" disabled={busy} onClick={() => run({ ids: [m.id] })}>
          <RotateCcwIcon data-icon="inline-start" />
          Replay
        </Button>
        <Button size="sm" variant="outline" disabled={busy || !payloads || original === undefined} onClick={() => setEditing((v) => !v)}>
          <PencilIcon data-icon="inline-start" />
          {editing ? "Close editor" : "Edit and replay"}
        </Button>
        <Confirm
          destructive
          title="Delete this entry?"
          description={<p>It is removed from the DLQ for good and will not be handled again.</p>}
          confirmLabel="Delete"
          onConfirm={() => run({ ids: [m.id] }, "DELETE_DLQ")}
          trigger={
            <Button size="sm" variant="ghost" className="ml-auto text-destructive" disabled={busy}>
              <Trash2Icon data-icon="inline-start" />
              Delete
            </Button>
          }
        />
      </div>
      {!payloads && (
        <p className="text-xs text-muted-foreground">
          Editing needs the payload, which the workers keep private (set RILLKIT_INSPECT_MESSAGES=true).
        </p>
      )}
      {editing && (
        <div className="flex flex-col gap-2">
          <label htmlFor="dlq-edit" className="text-sm font-medium">
            Payload to replay
          </label>
          <Textarea
            id="dlq-edit"
            value={text}
            onChange={(e) => setText(e.target.value)}
            className="max-h-80 min-h-40 font-mono text-xs"
            spellCheck={false}
            aria-invalid={!parsed.ok}
          />
          <div className="flex flex-wrap items-center gap-3">
            {parsed.ok ? (
              <Badge variant="running">Valid JSON</Badge>
            ) : (
              <span className="text-xs text-destructive" role="alert">
                Not valid JSON: {parsed.error}
              </span>
            )}
            <span className="text-xs text-muted-foreground">
              The worker's JSON codec decodes this as the message body. Only this entry changes.
            </span>
            <Button
              size="sm"
              className="ml-auto"
              disabled={busy || !parsed.ok || !changed}
              onClick={() => parsed.ok && run({ ids: [m.id], data: { [m.id]: JSON.stringify(parsed.value) } })}
            >
              <RotateCcwIcon data-icon="inline-start" />
              Replay edited
            </Button>
          </div>
        </div>
      )}
    </section>
  );
}
