import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

export function PageHeader({
  eyebrow,
  title,
  description,
  actions,
  className,
}: {
  eyebrow?: ReactNode;
  title: ReactNode;
  description?: ReactNode;
  actions?: ReactNode;
  className?: string;
}) {
  return (
    <header className={cn("flex flex-col gap-6 pt-10 pb-8 md:flex-row md:items-end md:justify-between", className)}>
      <div className="flex min-w-0 flex-col gap-3">
        {eyebrow && <div className="text-sm text-muted-foreground">{eyebrow}</div>}
        <h1 className="font-serif text-5xl leading-[1.02] font-normal tracking-tight text-balance md:text-6xl">
          {title}
        </h1>
        {description && <p className="max-w-2xl text-base text-muted-foreground">{description}</p>}
      </div>
      {actions && <div className="flex flex-wrap items-center gap-2">{actions}</div>}
    </header>
  );
}

export function SectionTitle({ children, aside }: { children: ReactNode; aside?: ReactNode }) {
  return (
    <div className="flex items-end justify-between gap-4 pb-4">
      <h2 className="font-serif text-3xl font-normal tracking-tight">{children}</h2>
      {aside}
    </div>
  );
}
