import { useState, type FormEvent } from "react";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Field, FieldDescription, FieldError, FieldGroup, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { Spinner } from "@/components/ui/spinner";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { errorMessage, useCreateStream } from "@/api/queries";
import { navigate, paths } from "@/lib/router";

/** Creates a stream and its consumer group (through a connected worker, or when one connects). */
export function NewStreamDialog({ tenant, open, onOpenChange }: { tenant: string; open: boolean; onOpenChange: (o: boolean) => void }) {
  const [stream, setStream] = useState("");
  const [group, setGroup] = useState("");
  const [start, setStart] = useState<"$" | "0">("$");
  const [error, setError] = useState<string | null>(null);
  const create = useCreateStream(tenant);

  function reset() {
    setStream("");
    setGroup("");
    setStart("$");
    setError(null);
    create.reset();
  }

  async function submit(e: FormEvent) {
    e.preventDefault();
    if (!stream.trim() || !group.trim()) {
      setError("Give the stream and its group a name.");
      return;
    }
    setError(null);
    try {
      const made = await create.mutateAsync({ stream: stream.trim(), group: group.trim(), start });
      onOpenChange(false);
      reset();
      navigate(paths.stream(tenant, made.stream.stream, made.stream.group));
    } catch (err) {
      setError(errorMessage(err));
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
      <DialogContent className="sm:max-w-lg">
        <form onSubmit={submit} className="flex flex-col gap-6">
          <DialogHeader>
            <DialogTitle className="font-serif text-2xl font-normal">New stream</DialogTitle>
            <DialogDescription>
              Creates the stream and a consumer group for it in Redis or Valkey, through a connected worker. Producers
              can publish to it straight away; workers subscribed to it start consuming.
            </DialogDescription>
          </DialogHeader>
          <FieldGroup>
            <Field data-invalid={Boolean(error) && !stream.trim()}>
              <FieldLabel htmlFor="new-stream">Stream</FieldLabel>
              <Input
                id="new-stream"
                placeholder="orders"
                value={stream}
                onChange={(e) => setStream(e.target.value)}
                className="font-mono"
                autoComplete="off"
                spellCheck={false}
                autoFocus
              />
            </Field>
            <Field data-invalid={Boolean(error) && !group.trim()}>
              <FieldLabel htmlFor="new-group">Group</FieldLabel>
              <Input
                id="new-group"
                placeholder="billing"
                value={group}
                onChange={(e) => setGroup(e.target.value)}
                className="font-mono"
                autoComplete="off"
                spellCheck={false}
              />
              <FieldDescription>The consumer group your workers subscribe with.</FieldDescription>
            </Field>
            <Field>
              <FieldLabel id="new-start-label">The group starts with</FieldLabel>
              <ToggleGroup
                type="single"
                variant="outline"
                value={start}
                onValueChange={(v) => v && setStart(v as "$" | "0")}
                aria-labelledby="new-start-label"
                className="w-full"
              >
                <ToggleGroupItem value="$" className="flex-1">
                  Only new messages
                </ToggleGroupItem>
                <ToggleGroupItem value="0" className="flex-1">
                  Everything already there
                </ToggleGroupItem>
              </ToggleGroup>
              <FieldDescription>
                Matters only if the stream already holds messages: with everything, the group handles those too.
              </FieldDescription>
            </Field>
            {error && <FieldError>{error}</FieldError>}
          </FieldGroup>
          <DialogFooter>
            <Button type="button" variant="ghost" onClick={() => onOpenChange(false)}>
              Cancel
            </Button>
            <Button type="submit" disabled={create.isPending}>
              {create.isPending && <Spinner data-icon="inline-start" />}
              Create stream
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
