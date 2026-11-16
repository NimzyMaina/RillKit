import { CircleDotIcon, PauseIcon, WrenchIcon, ArrowDownToLineIcon, CircleOffIcon } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import type { DirectiveStatus, WorkerState } from "@/api/types";

const STATE: Record<
  WorkerState,
  { variant: "running" | "paused" | "draining"; label: string; icon: typeof PauseIcon }
> = {
  RUNNING: { variant: "running", label: "Running", icon: CircleDotIcon },
  PAUSED: { variant: "paused", label: "Paused", icon: PauseIcon },
  MAINTENANCE: { variant: "paused", label: "Maintenance", icon: WrenchIcon },
  DRAINING: { variant: "draining", label: "Draining", icon: ArrowDownToLineIcon },
};

export function StateBadge({ state, className }: { state: WorkerState | null; className?: string }) {
  if (!state) {
    return (
      <Badge variant="outline" className={className}>
        <CircleOffIcon data-icon="inline-start" />
        Unknown
      </Badge>
    );
  }
  const s = STATE[state];
  const Icon = s.icon;
  return (
    <Badge variant={s.variant} className={className} data-state-badge={state}>
      <Icon data-icon="inline-start" aria-hidden />
      {s.label}
    </Badge>
  );
}

const DIRECTIVE: Record<
  DirectiveStatus,
  "outline" | "lavender" | "running" | "secondary" | "draining" | "paused"
> = {
  PENDING: "outline",
  SENT: "lavender",
  APPLIED: "running",
  DUPLICATE: "outline",
  REJECTED: "draining",
  FAILED: "draining",
  UNDELIVERABLE: "paused",
};

export function DirectiveStatusBadge({ status }: { status: DirectiveStatus }) {
  return (
    <Badge variant={DIRECTIVE[status]} className="font-mono text-[0.7rem] tracking-wide">
      {status}
    </Badge>
  );
}
