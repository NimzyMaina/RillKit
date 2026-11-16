import { cn } from "@/lib/utils";

/** The RillKit wordmark: a small ripple glyph and the name in EB Garamond. */
export function Wordmark({ className, size = "md" }: { className?: string; size?: "md" | "lg" }) {
  return (
    <span className={cn("inline-flex items-center gap-2 text-foreground", className)}>
      <svg
        viewBox="0 0 32 32"
        aria-hidden
        className={cn("shrink-0 text-brand", size === "lg" ? "size-9" : "size-7")}
      >
        <rect width="32" height="32" rx="10" fill="currentColor" />
        <path d="M7 19.5c3-5 6-5 9 0s6 5 9 0" fill="none" stroke="var(--background)" strokeWidth="2.4" strokeLinecap="round" />
        <path d="M7 12.5c3-5 6-5 9 0s6 5 9 0" fill="none" stroke="var(--lavender)" strokeWidth="2.4" strokeLinecap="round" />
      </svg>
      <span className={cn("font-serif leading-none tracking-tight", size === "lg" ? "text-4xl" : "text-[1.7rem]")}>
        Rill<em className="italic">Kit</em>
      </span>
    </span>
  );
}
