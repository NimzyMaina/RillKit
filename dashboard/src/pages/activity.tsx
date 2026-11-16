import { Fragment, useState } from "react";
import { BotIcon, ChevronLeftIcon, ChevronRightIcon, HistoryIcon, UserIcon } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardFooter } from "@/components/ui/card";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { PageHeader } from "@/components/page-header";
import { DirectiveStatusBadge, StateBadge } from "@/components/state-badge";
import { useDirectives } from "@/api/queries";
import type { Directive } from "@/api/types";
import { describe, targetLabel } from "@/lib/directives";
import { fmtDateTime, fmtInt, fmtTime, relativeTime } from "@/lib/format";
import { useNow } from "@/hooks/use-now";
import { cn } from "@/lib/utils";

type Filter = "all" | "user" | "autoscaler";
const PAGE = 25;

export function ActivityPage({ tenant }: { tenant: string }) {
  const [filter, setFilter] = useState<Filter>("all");
  const [pageIndex, setPageIndex] = useState(0);
  const { data, isPending, isFetching } = useDirectives(tenant, {
    limit: PAGE,
    offset: pageIndex * PAGE,
    source: filter === "all" ? undefined : filter,
  });
  const [open, setOpen] = useState<Set<string>>(new Set());
  const now = useNow(5000);
  const rows = data?.items ?? [];
  const total = data?.total ?? 0;
  const pages = Math.max(1, Math.ceil(total / PAGE));

  const toggle = (id: string) =>
    setOpen((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });

  return (
    <div className="flex flex-col gap-8">
      <PageHeader
        eyebrow="Directives, newest first"
        title={<em className="italic">Activity</em>}
        description="Every change sent to workers, by you or the autoscaler, and what the worker answered. Updates live."
        actions={
          <ToggleGroup
            type="single"
            variant="outline"
            value={filter}
            onValueChange={(v) => {
              if (!v) return;
              setFilter(v as Filter);
              setPageIndex(0);
            }}
            aria-label="Filter by source"
          >
            <ToggleGroupItem value="all" className="px-4">All</ToggleGroupItem>
            <ToggleGroupItem value="user" className="px-4">User</ToggleGroupItem>
            <ToggleGroupItem value="autoscaler" className="px-4">Autoscaler</ToggleGroupItem>
          </ToggleGroup>
        }
      />

      <Card className="py-2">
        <CardContent className="px-0">
          {isPending ? (
            <div className="flex flex-col gap-3 p-6">
              {Array.from({ length: 5 }, (_, i) => (
                <Skeleton key={i} className="h-10" />
              ))}
            </div>
          ) : rows.length === 0 ? (
            <Empty>
              <EmptyHeader>
                <EmptyMedia variant="icon">
                  <HistoryIcon />
                </EmptyMedia>
                <EmptyTitle>No directives yet</EmptyTitle>
                <EmptyDescription>Pause, resume or scale something and it shows up here.</EmptyDescription>
              </EmptyHeader>
            </Empty>
          ) : (
            <Table className="min-w-[52rem]">
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <TableHead className="w-10 pl-4">
                    <span className="sr-only">Details</span>
                  </TableHead>
                  <TableHead>Time</TableHead>
                  <TableHead>Directive</TableHead>
                  <TableHead>Scope / target</TableHead>
                  <TableHead>Source</TableHead>
                  <TableHead className="pr-6">Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {rows.map((d: Directive) => {
                  const expanded = open.has(d.id);
                  return (
                    <Fragment key={d.id}>
                      <TableRow
                        data-directive={d.id}
                        className={cn("cursor-pointer", expanded && "border-b-0 bg-muted/40")}
                        onClick={() => toggle(d.id)}
                      >
                        <TableCell className="pl-4">
                          <Button
                            variant="ghost"
                            size="icon-xs"
                            aria-expanded={expanded}
                            aria-controls={`dir-${d.id}`}
                            aria-label={expanded ? "Hide details" : "Show details"}
                            onClick={(e) => {
                              e.stopPropagation();
                              toggle(d.id);
                            }}
                          >
                            <ChevronRightIcon className={cn("transition-transform", expanded && "rotate-90")} />
                          </Button>
                        </TableCell>
                        <TableCell>
                          <div className="font-mono text-[0.8rem] font-numeric">{fmtTime(d.created_at)}</div>
                          <div className="text-xs text-muted-foreground">{relativeTime(d.created_at, now)}</div>
                        </TableCell>
                        <TableCell>
                          <div className="font-mono text-[0.8rem] font-medium">{d.cmd}</div>
                          <div className="text-xs text-muted-foreground">{describe(d)}</div>
                        </TableCell>
                        <TableCell>
                          <span className="flex items-center gap-2">
                            <Badge variant="outline">{d.scope}</Badge>
                            {d.scope !== "global" && <span className="font-mono text-[0.8rem]">{targetLabel(d)}</span>}
                          </span>
                        </TableCell>
                        <TableCell>
                          <span className="inline-flex items-center gap-1.5 text-sm">
                            {d.source === "autoscaler" ? (
                              <BotIcon className="size-4 text-brand" aria-hidden />
                            ) : (
                              <UserIcon className="size-4 text-muted-foreground" aria-hidden />
                            )}
                            {d.source === "autoscaler" ? "Autoscaler" : "User"}
                          </span>
                        </TableCell>
                        <TableCell className="pr-6">
                          <DirectiveStatusBadge status={d.status} />
                        </TableCell>
                      </TableRow>
                      {expanded && (
                        <TableRow id={`dir-${d.id}`} className="bg-muted/40 hover:bg-muted/40">
                          <TableCell colSpan={6} className="px-6 pt-0 pb-5 whitespace-normal">
                            <Details d={d} />
                          </TableCell>
                        </TableRow>
                      )}
                    </Fragment>
                  );
                })}
              </TableBody>
            </Table>
          )}
        </CardContent>
        {total > 0 && (
          <CardFooter className="justify-between gap-4 border-t px-6 pt-4 text-xs text-muted-foreground">
            <span className="font-numeric" data-testid="activity-page">
              Page {fmtInt(pageIndex + 1)} of {fmtInt(pages)} · {fmtInt(total)} {total === 1 ? "directive" : "directives"}
            </span>
            <div className="flex gap-2">
              <Button
                variant="outline"
                size="sm"
                disabled={pageIndex === 0 || isFetching}
                onClick={() => setPageIndex((p) => Math.max(0, p - 1))}
              >
                <ChevronLeftIcon data-icon="inline-start" />
                Previous
              </Button>
              <Button
                variant="outline"
                size="sm"
                disabled={pageIndex + 1 >= pages || isFetching}
                onClick={() => setPageIndex((p) => p + 1)}
              >
                Next
                <ChevronRightIcon data-icon="inline-end" />
              </Button>
            </div>
          </CardFooter>
        )}
      </Card>
    </div>
  );
}

function Details({ d }: { d: Directive }) {
  const rows: [string, React.ReactNode][] = [
    ["id", d.id],
    ["sent to", d.sent_to ?? "—"],
    ["created", fmtDateTime(d.created_at)],
    ["updated", fmtDateTime(d.updated_at)],
  ];
  if (d.reason) rows.push(["reason", d.reason]);
  return (
    <div className="grid gap-4 pl-10 md:grid-cols-3">
      <dl className="grid grid-cols-[auto_1fr] content-start gap-x-4 gap-y-1.5 text-xs">
        {rows.map(([k, v]) => (
          <div key={k} className="contents">
            <dt className="text-muted-foreground">{k}</dt>
            <dd className="font-mono break-all">{v}</dd>
          </div>
        ))}
      </dl>
      <JsonBlock title="Params" value={Object.keys(d.params).length ? d.params : null} />
      <div className="flex flex-col gap-2">
        <span className="text-xs font-medium text-muted-foreground">Ack</span>
        {d.ack ? (
          <div className="flex flex-col gap-2 rounded-xl border bg-card p-3 text-xs">
            <span className="font-mono">{d.ack.status}</span>
            {d.ack.error && <span className="text-destructive">{d.ack.error}</span>}
            {d.ack.current_state && (
              <span className="flex items-center gap-2">
                <StateBadge state={d.ack.current_state.state} />
                <span className="font-mono">{d.ack.current_state.concurrency} lanes</span>
              </span>
            )}
            {d.ack.result && (
              <pre className="font-mono text-[0.7rem] whitespace-pre-wrap">{JSON.stringify(d.ack.result, null, 2)}</pre>
            )}
          </div>
        ) : (
          <span className="text-xs text-muted-foreground">No ack yet.</span>
        )}
      </div>
    </div>
  );
}

function JsonBlock({ title, value }: { title: string; value: unknown }) {
  return (
    <div className="flex flex-col gap-2">
      <span className="text-xs font-medium text-muted-foreground">{title}</span>
      {value ? (
        <pre className="rounded-xl border bg-card p-3 font-mono text-[0.7rem] whitespace-pre-wrap">
          {JSON.stringify(value, null, 2)}
        </pre>
      ) : (
        <span className="text-xs text-muted-foreground">None.</span>
      )}
    </div>
  );
}
