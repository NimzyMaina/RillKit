import { useState, type FormEvent } from "react";
import { CheckIcon, CopyIcon, KeyRoundIcon, PlusIcon, TriangleAlertIcon } from "lucide-react";
import { toast } from "sonner";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import { Field, FieldDescription, FieldGroup, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Spinner } from "@/components/ui/spinner";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { Confirm } from "@/components/confirm";
import { PageHeader } from "@/components/page-header";
import { errorMessage, useApiKeys, useConnection, useCreateApiKey, useRevokeApiKey, useTenants } from "@/api/queries";
import type { ApiKey, NewApiKey } from "@/api/types";
import { fmtDateTime, relativeTime } from "@/lib/format";

async function copy(text: string, what: string) {
  try {
    await navigator.clipboard.writeText(text);
    toast.success(`${what} copied`);
    return true;
  } catch {
    toast.error("Could not copy", { description: "Select the text and copy it by hand." });
    return false;
  }
}

function CopyButton({ text, what, label }: { text: string; what: string; label?: string }) {
  const [done, setDone] = useState(false);
  return (
    <Button
      type="button"
      variant="outline"
      size={label ? "sm" : "icon-sm"}
      aria-label={label ? undefined : `Copy ${what.toLowerCase()}`}
      onClick={async () => {
        if (await copy(text, what)) {
          setDone(true);
          setTimeout(() => setDone(false), 1600);
        }
      }}
    >
      {done ? <CheckIcon data-icon={label ? "inline-start" : undefined} /> : <CopyIcon data-icon={label ? "inline-start" : undefined} />}
      {label}
    </Button>
  );
}

export function SettingsPage({ tenant }: { tenant: string }) {
  const { data: tenants } = useTenants();
  const t = tenants?.find((x) => x.id === tenant);
  return (
    <div className="flex flex-col gap-8">
      <PageHeader
        eyebrow={t ? `${t.name} · ${t.id}` : tenant}
        title={<em className="italic">Settings</em>}
        description="API keys let workers join this tenant's control plane. Give each deployment its own key."
      />
      <ApiKeysCard tenant={tenant} />
      <ConnectionCard tenant={tenant} />
    </div>
  );
}

function ApiKeysCard({ tenant }: { tenant: string }) {
  const { data: keys, isPending } = useApiKeys(tenant);
  const create = useCreateApiKey(tenant);
  const revoke = useRevokeApiKey(tenant);
  const [creating, setCreating] = useState(false);
  const [label, setLabel] = useState("");
  const [secret, setSecret] = useState<NewApiKey | null>(null);
  const [revoking, setRevoking] = useState<ApiKey | null>(null);

  async function submit(e: FormEvent) {
    e.preventDefault();
    try {
      const k = await create.mutateAsync(label.trim() || undefined);
      setCreating(false);
      setLabel("");
      setSecret(k);
    } catch {
      // Toasted by the mutation.
    }
  }

  const sorted = [...(keys ?? [])].sort(
    (a, b) => Number(a.revoked_at !== null) - Number(b.revoked_at !== null) || b.created_at.localeCompare(a.created_at),
  );

  return (
    <Card className="gap-2">
      <CardHeader>
        <CardTitle>API keys</CardTitle>
        <CardDescription>Workers present a key when they connect. Revoking one closes its connections at once.</CardDescription>
        <CardAction>
          <Button onClick={() => setCreating(true)}>
            <PlusIcon data-icon="inline-start" />
            New key
          </Button>
        </CardAction>
      </CardHeader>
      <CardContent className="px-0">
        {isPending ? (
          <div className="flex flex-col gap-3 p-6">
            <Skeleton className="h-10" />
            <Skeleton className="h-10" />
          </div>
        ) : sorted.length === 0 ? (
          <Empty>
            <EmptyHeader>
              <EmptyMedia variant="icon">
                <KeyRoundIcon />
              </EmptyMedia>
              <EmptyTitle>No API keys</EmptyTitle>
              <EmptyDescription>Create one to connect your first worker.</EmptyDescription>
            </EmptyHeader>
          </Empty>
        ) : (
          <Table className="min-w-[40rem]">
            <TableHeader>
              <TableRow className="hover:bg-transparent">
                <TableHead className="pl-6">Key</TableHead>
                <TableHead>Label</TableHead>
                <TableHead>Created</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="pr-6 text-right">
                  <span className="sr-only">Actions</span>
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {sorted.map((k) => (
                <TableRow key={k.id} data-key={k.id} className={k.revoked_at ? "text-muted-foreground" : undefined}>
                  <TableCell className="pl-6 font-mono text-[0.8rem]">{k.prefix}…</TableCell>
                  <TableCell>{k.label ?? <span className="text-muted-foreground">No label</span>}</TableCell>
                  <TableCell>
                    <Tooltip>
                      <TooltipTrigger className="rounded underline decoration-dotted decoration-border underline-offset-4 outline-none focus-visible:ring-3 focus-visible:ring-ring/50">
                        {relativeTime(k.created_at)}
                      </TooltipTrigger>
                      <TooltipContent>{fmtDateTime(k.created_at)}</TooltipContent>
                    </Tooltip>
                  </TableCell>
                  <TableCell>
                    {k.revoked_at ? (
                      <Badge variant="outline">Revoked {relativeTime(k.revoked_at)}</Badge>
                    ) : (
                      <Badge variant="running">Active</Badge>
                    )}
                  </TableCell>
                  <TableCell className="pr-6 text-right">
                    {!k.revoked_at && (
                      <Button variant="ghost" size="sm" onClick={() => setRevoking(k)} aria-label={`Revoke key ${k.prefix}`}>
                        Revoke
                      </Button>
                    )}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </CardContent>

      <Dialog
        open={creating}
        onOpenChange={(o) => {
          setCreating(o);
          if (!o) setLabel("");
        }}
      >
        <DialogContent>
          <form onSubmit={submit} className="flex flex-col gap-6">
            <DialogHeader>
              <DialogTitle>New API key</DialogTitle>
              <DialogDescription>The secret is shown once, right after you create it.</DialogDescription>
            </DialogHeader>
            <FieldGroup>
              <Field>
                <FieldLabel htmlFor="key-label">Label (optional)</FieldLabel>
                <Input id="key-label" value={label} onChange={(e) => setLabel(e.target.value)} placeholder="production workers" />
                <FieldDescription>Helps you tell keys apart later.</FieldDescription>
              </Field>
            </FieldGroup>
            <DialogFooter>
              <Button type="button" variant="outline" onClick={() => setCreating(false)}>
                Cancel
              </Button>
              <Button type="submit" disabled={create.isPending}>
                {create.isPending && <Spinner data-icon="inline-start" />}
                Create key
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      <Dialog open={secret !== null} onOpenChange={(o) => !o && setSecret(null)}>
        <DialogContent onInteractOutside={(e) => e.preventDefault()} data-testid="secret-dialog">
          <DialogHeader>
            <DialogTitle>Copy your new key</DialogTitle>
            <DialogDescription>
              {secret?.label ? <>Key “{secret.label}” is ready.</> : "Your key is ready."} Set it as{" "}
              <code className="font-mono">RILLKIT_API_KEY</code> on your workers.
            </DialogDescription>
          </DialogHeader>
          <div className="flex items-center gap-2 rounded-2xl border bg-muted/50 p-2 pl-4">
            <code className="min-w-0 flex-1 font-mono text-[0.8rem] break-all select-all" data-testid="secret-value">
              {secret?.key}
            </code>
            {secret && <CopyButton text={secret.key} what="API key" />}
          </div>
          <Alert className="border-amber/60 bg-amber/15">
            <TriangleAlertIcon />
            <AlertTitle>This is the only time you will see it</AlertTitle>
            <AlertDescription>
              The server keeps only a hash. Store the key in your secret manager now; if you lose it, revoke it and
              create another.
            </AlertDescription>
          </Alert>
          <DialogFooter>
            <Button onClick={() => setSecret(null)}>I have stored it</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Confirm
        open={revoking !== null}
        onOpenChange={(o) => !o && setRevoking(null)}
        destructive
        title={`Revoke ${revoking?.prefix ?? ""}…?`}
        description={
          <p>
            Workers using {revoking?.label ? `“${revoking.label}”` : "this key"} are disconnected now and cannot
            reconnect with it. They keep consuming under the last state they received.
          </p>
        }
        confirmLabel="Revoke key"
        onConfirm={() => revoking && revoke.mutate(revoking.id)}
      />
    </Card>
  );
}

const SECRET_NAME = /KEY|SECRET|TOKEN|PASSWORD/i;

/** Renders the worker env as a .env block; anything secret-looking is replaced by a placeholder. */
function envBlock(env: Record<string, string>): string {
  const lines = Object.entries(env)
    .filter(([k]) => k !== "RILLKIT_API_KEY")
    .map(([k, v]) => `${k}=${SECRET_NAME.test(k) ? "<secret>" : v}`);
  lines.push("RILLKIT_API_KEY=<paste your key>");
  return lines.join("\n");
}

function ConnectionCard({ tenant }: { tenant: string }) {
  const { data, isPending, error } = useConnection(tenant);
  const text = data ? envBlock(data.env) : "";

  return (
    <Card>
      <CardHeader>
        <CardTitle>Connect a worker</CardTitle>
        <CardDescription>
          Put these variables in the worker's environment, then add the API key yourself. Secrets are never shown
          here.
        </CardDescription>
        {data && (
          <CardAction>
            <CopyButton text={text} what="Environment" label="Copy" />
          </CardAction>
        )}
      </CardHeader>
      <CardContent>
        {isPending ? (
          <Skeleton className="h-36 rounded-2xl" />
        ) : error ? (
          <Alert variant="destructive">
            <AlertTitle>Could not load the connection settings</AlertTitle>
            <AlertDescription>{errorMessage(error)}</AlertDescription>
          </Alert>
        ) : (
          <div className="overflow-hidden rounded-2xl bg-[#1a1a1a] text-[#ffffeb] dark:border dark:bg-black/40">
            <div className="flex items-center justify-between border-b border-white/10 px-4 py-2">
              <span className="font-mono text-xs text-[#ffffeb]/70">.env</span>
              <span className="font-mono text-xs text-[#ffffeb]/70">{data?.control_url}</span>
            </div>
            <pre className="overflow-x-auto p-4 font-mono text-[0.8rem] leading-relaxed" data-testid="env-block">
              {text.split("\n").map((line) => {
                const i = line.indexOf("=");
                const k = line.slice(0, i);
                const v = line.slice(i + 1);
                return (
                  <div key={k}>
                    <span className="text-[#f0d7ff]">{k}</span>
                    <span className="text-[#ffffeb]/60">=</span>
                    <span className={v.startsWith("<") ? "text-[#ffbcf2] italic" : undefined}>{v}</span>
                  </div>
                );
              })}
            </pre>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
