import { useState, type FormEvent, type ReactNode } from "react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Field, FieldDescription, FieldGroup, FieldLabel } from "@/components/ui/field";
import { WorkersSlider } from "@/components/workers-slider";

/** A dialog that picks a lane count (1..256) for a node. */
export function ConcurrencyDialog({
  open,
  onOpenChange,
  title,
  description,
  initial,
  onApply,
}: {
  open: boolean;
  onOpenChange: (o: boolean) => void;
  title: ReactNode;
  description: ReactNode;
  initial: number;
  onApply: (n: number) => void;
}) {
  const [value, setValue] = useState(initial);

  function submit(e: FormEvent) {
    e.preventDefault();
    onApply(value);
    onOpenChange(false);
  }

  return (
    <Dialog
      open={open}
      onOpenChange={(o) => {
        if (o) setValue(initial);
        onOpenChange(o);
      }}
    >
      <DialogContent>
        <form onSubmit={submit} className="flex flex-col gap-6">
          <DialogHeader>
            <DialogTitle>{title}</DialogTitle>
            <DialogDescription>{description}</DialogDescription>
          </DialogHeader>
          <FieldGroup>
            <Field>
              <FieldLabel htmlFor="node-concurrency">Workers (lanes) on this node</FieldLabel>
              <WorkersSlider id="node-concurrency" value={value} onChange={setValue} />
              <FieldDescription>Every subscription on the node runs this many lanes. 1 to 256.</FieldDescription>
            </Field>
          </FieldGroup>
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
              Cancel
            </Button>
            <Button type="submit">Apply {value}</Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
