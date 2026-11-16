import { Fragment, useCallback, useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import { ChevronDownIcon, InboxIcon, RefreshCwIcon, SearchIcon } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Card, CardAction, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Skeleton } from "@/components/ui/skeleton";
import { Spinner } from "@/components/ui/spinner";
import { Switch } from "@/components/ui/switch";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { MessageDetail, MessageStatusBadge, PrivacyNote } from "@/components/message-detail";
import { api, errorMessage, errorTitle } from "@/api/queries";
import type { StreamMessage, StreamSummary } from "@/api/types";
import { fmtBytes, fmtInt, fmtTime, idTime } from "@/lib/format";
import { cn } from "@/lib/utils";

const PAGE = 50;
const POLL_MS = 2000;
const STATUS_EVERY = 3; // polls between refreshing the status of the rows already shown
const MAX_ROWS = 500;
const ENTRY_ID = /^\d+-\d+$/;

function preview(m: StreamMessage): string {
  const data = m.fields?.data;
  if (data === undefined) return "";
  return data.length > 120 ? `${data.slice(0, 120)}…` : data;
}

/**
 * The stream's messages, newest first. It opens as a snapshot "as of" the time it was read;
 * turning Live on appends what arrives after that, on top, without replacing the list.
 */
export function MessagesTab({ tenant, s }: { tenant: string; s: StreamSummary }) {
  const [rows, setRows] = useState<StreamMessage[]>([]);
  const [asOf, setAsOf] = useState<string | null>(null);
  const [nextBefore, setNextBefore] = useState<string | null>(null);
  const [total, setTotal] = useState<number | null>(null);
  const [payloads, setPayloads] = useState(s.capabilities.inspect_messages);
  const [loading, setLoading] = useState(true);
  const [olderLoading, setOlderLoading] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [live, setLive] = useState(false);
  const [newCount, setNewCount] = useState(0);
  const [filter, setFilter] = useState("");
  const [selected, setSelected] = useState<StreamMessage | null>(null);
  const [lookup, setLookup] = useState<{ id: string; loading: boolean; error: unknown } | null>(null);

  const rowsRef = useRef(rows);
  rowsRef.current = rows;

  const snapshot = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const page = await api().listMessages(tenant, s.stream, s.group, { count: PAGE });
      setRows(page.messages);
      setAsOf(page.as_of);
      setNextBefore(page.next_before);
      setTotal(page.total);
      setPayloads(page.payloads);
      setNewCount(0);
    } catch (e) {
      setError(e);
    } finally {
      setLoading(false);
    }
  }, [tenant, s.stream, s.group]);

  useEffect(() => {
    void snapshot();
  }, [snapshot]);

  // Live: append what arrived after the newest row; now and then refresh the shown statuses.
  useEffect(() => {
    if (!live) return;
    let stopped = false;
    let polls = 0;
    const poll = async () => {
      polls += 1;
      try {
        let added: StreamMessage[] = [];
        let more = true;
        let lastAsOf: string | null = null;
        let lastTotal: number | null = null;
        for (let i = 0; more && i < 10 && !stopped; i++) {
          const newest = added[0]?.id ?? rowsRef.current[0]?.id ?? null;
          const page = await api().listMessages(tenant, s.stream, s.group, {
            count: PAGE,
            ...(newest ? { after: newest } : {}),
          });
          added = [...page.messages, ...added];
          more = Boolean(newest) && page.more_after;
          lastAsOf = page.as_of;
          lastTotal = page.total;
          setPayloads(page.payloads);
        }
        let statuses: Map<string, StreamMessage> | null = null;
        if (polls % STATUS_EVERY === 0 && rowsRef.current.length > 0) {
          const recent = await api().listMessages(tenant, s.stream, s.group, {
            count: Math.min(rowsRef.current.length + added.length, 200),
          });
          statuses = new Map(recent.messages.map((m) => [m.id, m]));
        }
        if (stopped) return;
        setRows((prev) => {
          const seen = new Set(prev.map((m) => m.id));
          const fresh = added.filter((m) => !seen.has(m.id));
          if (fresh.length) setNewCount((n) => n + fresh.length);
          let next = [...fresh, ...prev];
          if (statuses) {
            next = next.map((m) => {
              const u = statuses!.get(m.id);
              return u ? { ...m, status: u.status, deliveries: u.deliveries, consumer: u.consumer, idle_ms: u.idle_ms } : m;
            });
          }
          return next.slice(0, MAX_ROWS);
        });
        if (lastAsOf) setAsOf(lastAsOf);
        if (lastTotal !== null) setTotal(lastTotal);
        setError(null);
      } catch (e) {
        if (!stopped) setError(e);
      }
    };
    void poll();
    const timer = setInterval(() => void poll(), POLL_MS);
    return () => {
      stopped = true;
      clearInterval(timer);
    };
  }, [live, tenant, s.stream, s.group]);

  async function loadOlder() {
    if (!nextBefore) return;
    setOlderLoading(true);
    try {
      const page = await api().listMessages(tenant, s.stream, s.group, { count: PAGE, before: nextBefore });
      setRows((prev) => {
        const seen = new Set(prev.map((m) => m.id));
        return [...prev, ...page.messages.filter((m) => !seen.has(m.id))];
      });
      setNextBefore(page.next_before);
      setTotal(page.total);
    } catch (e) {
      setError(e);
    } finally {
      setOlderLoading(false);
    }
  }

  async function search(e: FormEvent) {
    e.preventDefault();
    const id = filter.trim();
    if (!ENTRY_ID.test(id)) return; // anything else filters the rows as you type
    const shown = rows.find((m) => m.id === id);
    if (shown) {
      setSelected(shown);
      return;
    }
    setLookup({ id, loading: true, error: null });
    setSelected(null);
    try {
      const m = await api().getMessage(tenant, s.stream, s.group, id, "stream");
      setLookup(null);
      setSelected(m);
    } catch (err) {
      setLookup({ id, loading: false, error: err });
    }
  }

  const visible = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q || ENTRY_ID.test(q)) return rows;
    return rows.filter((m) => m.id.includes(q) || (m.fields?.data ?? "").toLowerCase().includes(q));
  }, [rows, filter]);

  const openId = selected?.id ?? null;
  // Keep the open detail current while live polling refreshes statuses under it.
  const openMessage = selected ? rows.find((m) => m.id === selected.id) ?? selected : null;
  const closeDetail = () => {
    setSelected(null);
    setLookup(null);
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle>Messages</CardTitle>
        <CardDescription>
          The newest entries of <span className="font-mono">{s.stream_key ?? s.stream}</span>, newest first, with their
          state in group <span className="font-mono">{s.group}</span>.
        </CardDescription>
        <CardAction className="flex items-center gap-4">
          <div className="flex items-center gap-2" aria-live="polite">
            {live ? (
              <span className="flex items-center gap-2 text-xs text-muted-foreground" data-testid="live-status">
                <span className="relative flex size-2">
                  <span className="absolute inline-flex size-full animate-ping rounded-full bg-state-running opacity-60" />
                  <span className="relative inline-flex size-2 rounded-full bg-state-running" />
                </span>
                Live · {fmtInt(newCount)} new
              </span>
            ) : (
              asOf && (
                <span className="text-xs text-muted-foreground font-numeric" data-testid="as-of">
                  As of {fmtTime(asOf)}
                </span>
              )
            )}
            {!live && (
              <Button variant="ghost" size="icon-sm" aria-label="Refresh" onClick={() => void snapshot()} disabled={loading}>
                <RefreshCwIcon className={cn(loading && "animate-spin")} />
              </Button>
            )}
          </div>
          <div className="flex items-center gap-2">
            <Switch id="messages-live" checked={live} onCheckedChange={setLive} />
            <Label htmlFor="messages-live">Live</Label>
          </div>
        </CardAction>
      </CardHeader>

      <CardContent className="flex flex-col gap-4">
        <form onSubmit={search} className="flex flex-col gap-2 sm:flex-row" role="search">
          <Label htmlFor="message-search" className="sr-only">
            Search messages
          </Label>
          <div className="relative flex-1">
            <SearchIcon className="pointer-events-none absolute top-1/2 left-3 size-4 -translate-y-1/2 text-muted-foreground" aria-hidden />
            <Input
              id="message-search"
              placeholder="Find a message by id (1718000000000-0), or filter by text"
              value={filter}
              onChange={(e) => {
                setFilter(e.target.value);
                setLookup(null);
              }}
              className="pl-9 font-mono"
              autoComplete="off"
              spellCheck={false}
            />
          </div>
          <Button type="submit" variant="outline" disabled={!ENTRY_ID.test(filter.trim())}>
            Open message
          </Button>
        </form>

        {!payloads && (
          <PrivacyNote what="Payloads are hidden" variable="RILLKIT_INSPECT_MESSAGES">
            You see ids, sizes and delivery state; the workers keep message contents private.
          </PrivacyNote>
        )}

        {error ? (
          <Alert variant="destructive">
            <AlertTitle>{errorTitle(error)}</AlertTitle>
            <AlertDescription>{errorMessage(error)}</AlertDescription>
          </Alert>
        ) : null}

        {(lookup || (openMessage && !visible.some((m) => m.id === openMessage.id))) && (
          <div className="overflow-hidden rounded-2xl border">
            <MessageDetail
              key={lookup?.id ?? openMessage?.id ?? "lookup"}
              tenant={tenant}
              stream={s.stream}
              message={openMessage}
              loading={lookup?.loading}
              error={lookup?.error}
              payloads={payloads}
              captureLogs={s.capabilities.capture_logs}
              traceRecords={s.capabilities.trace_records}
              onClose={closeDetail}
            />
          </div>
        )}

        {loading && rows.length === 0 ? (
          <div className="flex flex-col gap-2">
            {Array.from({ length: 6 }, (_, i) => (
              <Skeleton key={i} className="h-10 rounded-xl" />
            ))}
          </div>
        ) : visible.length === 0 ? (
          <Empty className="py-10">
            <EmptyHeader>
              <EmptyMedia variant="icon">
                <InboxIcon />
              </EmptyMedia>
              <EmptyTitle>{rows.length === 0 ? "No messages yet" : "Nothing matches"}</EmptyTitle>
              <EmptyDescription>
                {rows.length === 0
                  ? "Nothing has been published to this stream, or it was trimmed."
                  : "No loaded message has that text. Paste a full message id to look it up."}
              </EmptyDescription>
            </EmptyHeader>
          </Empty>
        ) : (
          <div className="overflow-x-auto rounded-2xl border">
            <Table data-testid="messages-table">
              <TableHeader>
                <TableRow>
                  <TableHead className="w-24">Time</TableHead>
                  <TableHead>Message id</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Consumer</TableHead>
                  <TableHead className="text-right">Size</TableHead>
                  {payloads && <TableHead className="w-full">Payload</TableHead>}
                  <TableHead className="w-8" />
                </TableRow>
              </TableHeader>
              <TableBody>
                {visible.map((m) => {
                  const expanded = openId === m.id;
                  return (
                    <Fragment key={m.id}>
                      <TableRow
                        className="cursor-pointer"
                        tabIndex={0}
                        aria-expanded={expanded}
                        data-message-row
                        onClick={() => setSelected(expanded ? null : m)}
                        onKeyDown={(e) => {
                          if (e.key === "Enter" || e.key === " ") {
                            e.preventDefault();
                            setSelected(expanded ? null : m);
                          }
                        }}
                      >
                        <TableCell className="font-mono text-xs text-muted-foreground font-numeric">{fmtTime(idTime(m.id))}</TableCell>
                        <TableCell className="font-mono text-xs">{m.id}</TableCell>
                        <TableCell>
                          <MessageStatusBadge m={m} />
                        </TableCell>
                        <TableCell className="max-w-48 truncate font-mono text-xs text-muted-foreground">{m.consumer ?? ""}</TableCell>
                        <TableCell className="text-right font-mono text-xs font-numeric">{fmtBytes(m.size)}</TableCell>
                        {payloads && (
                          <TableCell className="max-w-0 truncate font-mono text-xs text-muted-foreground">{preview(m)}</TableCell>
                        )}
                        <TableCell className="text-right">
                          <ChevronDownIcon
                            className={cn("ml-auto size-4 text-muted-foreground transition-transform", expanded && "rotate-180")}
                            aria-hidden
                          />
                        </TableCell>
                      </TableRow>
                      {expanded && (
                        <TableRow className="hover:bg-transparent" data-detail-row>
                          <TableCell colSpan={payloads ? 7 : 6} className="p-0">
                            <MessageDetail
                              key={m.id}
                              tenant={tenant}
                              stream={s.stream}
                                              message={openMessage?.id === m.id ? openMessage : m}
                              payloads={payloads}
                              captureLogs={s.capabilities.capture_logs}
                              traceRecords={s.capabilities.trace_records}
                              onClose={closeDetail}
                            />
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
        <span className="font-numeric">
          Showing {fmtInt(visible.length)}
          {total !== null ? ` of ${fmtInt(total)} in the stream` : ""}
        </span>
        {!live && nextBefore && (
          <Button variant="outline" size="sm" onClick={() => void loadOlder()} disabled={olderLoading}>
            {olderLoading && <Spinner data-icon="inline-start" />}
            Load older
          </Button>
        )}
      </CardFooter>
    </Card>
  );
}
