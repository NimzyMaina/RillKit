import { useState, type FormEvent } from "react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Field, FieldContent, FieldDescription, FieldError, FieldGroup, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import { Spinner } from "@/components/ui/spinner";
import { ApiError } from "@/api/client";
import { errorMessage, useCreateTenant } from "@/api/queries";
import { navigate, paths } from "@/lib/router";

const ID_RE = /^[a-z0-9-]{1,40}$/;

export function NewTenantDialog({ open, onOpenChange }: { open: boolean; onOpenChange: (o: boolean) => void }) {
  const [id, setId] = useState("");
  const [name, setName] = useState("");
  const [enforce, setEnforce] = useState(false);
  const [error, setError] = useState<{ field: "id" | "name" | "form"; msg: string } | null>(null);
  const create = useCreateTenant();

  function reset() {
    setId("");
    setName("");
    setEnforce(false);
    setError(null);
    create.reset();
  }

  async function submit(e: FormEvent) {
    e.preventDefault();
    if (!ID_RE.test(id)) {
      setError({ field: "id", msg: "Use 1 to 40 lowercase letters, digits or -." });
      return;
    }
    if (!name.trim()) {
      setError({ field: "name", msg: "Give the tenant a name." });
      return;
    }
    setError(null);
    try {
      const t = await create.mutateAsync({ id, name: name.trim(), enforce_keys: enforce });
      toast.success(`Tenant ${t.name} created`);
      onOpenChange(false);
      reset();
      navigate(paths.overview(t.id));
    } catch (err) {
      const field = err instanceof ApiError && err.code === "CONFLICT" ? "id" : "form";
      setError({ field, msg: errorMessage(err) });
    }
  }

  return (
    <Dialog
      open={open}
      onOpenChange={(o) => {
        onOpenChange(o);
        if (!o) reset();
      }}
    >
      <DialogContent>
        <form onSubmit={submit} className="flex flex-col gap-6">
          <DialogHeader>
            <DialogTitle>New tenant</DialogTitle>
            <DialogDescription>A tenant is an isolated set of workers, streams and API keys.</DialogDescription>
          </DialogHeader>
          <FieldGroup>
            <Field data-invalid={error?.field === "id" || undefined}>
              <FieldLabel htmlFor="tenant-id">Id</FieldLabel>
              <Input
                id="tenant-id"
                value={id}
                onChange={(e) => setId(e.target.value.toLowerCase())}
                placeholder="t-42"
                className="font-mono"
                autoComplete="off"
                spellCheck={false}
                maxLength={40}
                aria-invalid={error?.field === "id" || undefined}
              />
              {error?.field === "id" ? (
                <FieldError>{error.msg}</FieldError>
              ) : (
                <FieldDescription>Lowercase letters, digits and -. It cannot be changed later.</FieldDescription>
              )}
            </Field>
            <Field data-invalid={error?.field === "name" || undefined}>
              <FieldLabel htmlFor="tenant-name">Name</FieldLabel>
              <Input
                id="tenant-name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="Northwind Cloud"
                aria-invalid={error?.field === "name" || undefined}
              />
              {error?.field === "name" && <FieldError>{error.msg}</FieldError>}
            </Field>
            <Field orientation="horizontal" className="rounded-2xl border bg-muted/40 p-4">
              <FieldContent>
                <FieldLabel htmlFor="tenant-enforce">Enforce keys</FieldLabel>
                <FieldDescription>
                  Cloud mode: workers must use hash-tagged keys with prefix <code className="font-mono">rk</code> and
                  this tenant id.
                </FieldDescription>
              </FieldContent>
              <Switch id="tenant-enforce" checked={enforce} onCheckedChange={setEnforce} />
            </Field>
            {error?.field === "form" && <FieldError>{error.msg}</FieldError>}
          </FieldGroup>
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
              Cancel
            </Button>
            <Button type="submit" disabled={create.isPending}>
              {create.isPending && <Spinner data-icon="inline-start" />}
              Create tenant
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
