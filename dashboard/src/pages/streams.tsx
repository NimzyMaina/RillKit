import { useMemo, useState } from "react";
import { ArrowUpRightIcon, ChevronLeftIcon, ChevronRightIcon, PlusIcon, SearchIcon, WavesIcon } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardFooter } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { NewStreamDialog } from "@/components/new-stream-dialog";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { PageHeader } from "@/components/page-header";
import { StateBadge } from "@/components/state-badge";
import { useOverview } from "@/api/queries";
import { fmtCompact, fmtInt, fmtRate } from "@/lib/format";
import { Link, navigate, paths } from "@/lib/router";

const PAGE = 20;

export function StreamsPage({ tenant }: { tenant: string }) {
  const { data: ov, isPending } = useOverview(tenant);
  const all = ov?.streams ?? [];
  const totalLag = all.reduce((a, s) => a + (s.lag ?? 0), 0);
  const [query, setQuery] = useState("");
  const [pageIndex, setPageIndex] = useState(0);
  const [creating, setCreating] = useState(false);
  const matching = useMemo(() => {
    const q = query.trim().toLowerCase();
    return q ? all.filter((s) => s.stream.toLowerCase().includes(q) || s.group.toLowerCase().includes(q)) : all;
  }, [all, query]);
  const pages = Math.max(1, Math.ceil(matching.length / PAGE));
  const page = Math.min(pageIndex, pages - 1);
  const streams = matching.slice(page * PAGE, page * PAGE + PAGE);

  return (
    <div className="flex flex-col gap-8">
      <PageHeader
        eyebrow="Consumer groups across every node"
        title={
          <>
            <em className="italic">Streams</em>
          </>
        }
        description={
          ov ? (
            <>
              {all.length} {all.length === 1 ? "stream" : "streams"} with{" "}
              <span className="font-mono font-numeric text-foreground">{fmtInt(totalLag)}</span> messages waiting.
            </>
          ) : undefined
        }
        actions={
          <Button onClick={() => setCreating(true)}>
            <PlusIcon data-icon="inline-start" />
            New stream
          </Button>
        }
      />
      <NewStreamDialog tenant={tenant} open={creating} onOpenChange={setCreating} />

      <div className="relative max-w-md">
        <Label htmlFor="stream-search" className="sr-only">
          Search streams
        </Label>
        <SearchIcon className="pointer-events-none absolute top-1/2 left-3 size-4 -translate-y-1/2 text-muted-foreground" aria-hidden />
        <Input
          id="stream-search"
          type="search"
          placeholder="Search streams and groups"
          value={query}
          onChange={(e) => {
            setQuery(e.target.value);
            setPageIndex(0);
          }}
          className="pl-9"
        />
      </div>

      <Card className="py-2">
        <CardContent className="px-0">
          {isPending ? (
            <div className="flex flex-col gap-3 p-6">
              {Array.from({ length: 3 }, (_, i) => (
                <Skeleton key={i} className="h-10" />
              ))}
            </div>
          ) : streams.length === 0 ? (
            <Empty>
              <EmptyHeader>
                <EmptyMedia variant="icon">
                  <WavesIcon />
                </EmptyMedia>
                <EmptyTitle>{all.length === 0 ? "No streams yet" : "No stream matches"}</EmptyTitle>
                <EmptyDescription>
                  {all.length === 0
                    ? "Streams appear here once a connected worker subscribes to one, or when you create one."
                    : `Nothing is called “${query.trim()}”.`}
                </EmptyDescription>
              </EmptyHeader>
            </Empty>
          ) : (
            <Table className="min-w-[60rem]">
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <TableHead className="pl-6">Stream</TableHead>
                  <TableHead>Group</TableHead>
                  <TableHead>State</TableHead>
                  <TableHead className="text-right">Lag</TableHead>
                  <TableHead className="text-right">Pending</TableHead>
                  <TableHead className="text-right">Ack / s</TableHead>
                  <TableHead className="text-right">Workers</TableHead>
                  <TableHead className="text-right">Nodes</TableHead>
                  <TableHead className="text-right">DLQ</TableHead>
                  <TableHead className="pr-6">Autoscaling</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {streams.map((s) => {
                  const href = paths.stream(tenant, s.stream, s.group);
                  return (
                    <TableRow
                      key={`${s.stream}/${s.group}`}
                      className="group cursor-pointer"
                      onClick={(e) => {
                        if ((e.target as HTMLElement).closest("a")) return;
                        navigate(href);
                      }}
                      data-stream={s.stream}
                    >
                      <TableCell className="pl-6">
                        <Link
                          href={href}
                          className="inline-flex items-center gap-1.5 rounded font-mono text-[0.85rem] font-medium outline-none hover:text-brand focus-visible:ring-3 focus-visible:ring-ring/50"
                        >
                          {s.stream}
                          <ArrowUpRightIcon className="size-3.5 opacity-0 transition-opacity group-hover:opacity-100" aria-hidden />
                        </Link>
                        <div className="font-mono text-[0.7rem] text-muted-foreground">{s.stream_key ?? "not consumed yet"}</div>
                      </TableCell>
                      <TableCell className="font-mono text-[0.8rem]">{s.group}</TableCell>
                      <TableCell>
                        {s.nodes > 0 ? <StateBadge state={s.state} /> : <Badge variant="outline">No consumers</Badge>}
                      </TableCell>
                      <TableCell className="text-right font-mono font-numeric">{fmtCompact(s.lag)}</TableCell>
                      <TableCell className="text-right font-mono font-numeric">{fmtCompact(s.pending)}</TableCell>
                      <TableCell className="text-right font-mono font-numeric">{fmtRate(s.ack_rate_sec)}</TableCell>
                      <TableCell className="text-right font-mono font-numeric">{s.concurrency}</TableCell>
                      <TableCell className="text-right font-mono font-numeric">{s.nodes}</TableCell>
                      <TableCell className="text-right font-mono font-numeric">
                        {s.dlq_length ? <span className="text-state-draining">{fmtCompact(s.dlq_length)}</span> : fmtCompact(s.dlq_length)}
                      </TableCell>
                      <TableCell className="pr-6">
                        {s.policy?.enabled ? (
                          <Badge variant="running">
                            On · {s.policy.min_workers}–{s.policy.max_workers}
                          </Badge>
                        ) : (
                          <Badge variant="outline">{s.policy ? "Disabled" : "Off"}</Badge>
                        )}
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          )}
        </CardContent>
        {matching.length > PAGE && (
          <CardFooter className="justify-between gap-4 border-t px-6 pt-4 text-xs text-muted-foreground">
            <span className="font-numeric" data-testid="streams-page">
              Page {page + 1} of {pages} · {fmtInt(matching.length)} streams
            </span>
            <div className="flex gap-2">
              <Button variant="outline" size="sm" disabled={page === 0} onClick={() => setPageIndex(page - 1)}>
                <ChevronLeftIcon data-icon="inline-start" />
                Previous
              </Button>
              <Button variant="outline" size="sm" disabled={page + 1 >= pages} onClick={() => setPageIndex(page + 1)}>
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
