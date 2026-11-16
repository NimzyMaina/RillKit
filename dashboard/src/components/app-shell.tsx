import type { ReactNode } from "react";
import { LogOutIcon, MoonIcon, SunIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { Wordmark } from "@/components/wordmark";
import { TenantSwitcher } from "@/components/tenant-switcher";
import { LiveProvider, useLiveStatus } from "@/api/live";
import { IS_MOCK } from "@/api";
import { clearToken } from "@/lib/auth";
import { Link, paths, useRoute } from "@/lib/router";
import { useTheme } from "@/lib/theme";
import { cn } from "@/lib/utils";

export function AppShell({ tenant, children }: { tenant: string; children: ReactNode }) {
  return (
    <LiveProvider tenant={tenant}>
      <div className="flex min-h-svh flex-col">
        <TopBar tenant={tenant} />
        <main id="main" className="mx-auto w-full max-w-7xl flex-1 px-5 pb-24 md:px-10">
          {children}
        </main>
      </div>
    </LiveProvider>
  );
}

function TopBar({ tenant }: { tenant: string }) {
  const route = useRoute();
  const { theme, toggle } = useTheme();

  const nav = [
    { label: "Overview", href: paths.overview(tenant), active: route.name === "overview" },
    { label: "Streams", href: paths.streams(tenant), active: route.name === "streams" || route.name === "stream" },
    { label: "Activity", href: paths.activity(tenant), active: route.name === "activity" },
    { label: "Settings", href: paths.settings(tenant), active: route.name === "settings" },
  ];

  return (
    <header className="sticky top-0 z-40 border-b bg-background/85 backdrop-blur-md">
      <a
        href="#main"
        className="sr-only focus:not-sr-only focus:absolute focus:top-2 focus:left-2 focus:rounded-full focus:bg-primary focus:px-4 focus:py-2 focus:text-primary-foreground"
      >
        Skip to content
      </a>
      <div className="mx-auto flex h-16 max-w-7xl items-center gap-4 px-5 md:px-10">
        <Link href={paths.overview(tenant)} aria-label="RillKit, overview" className="rounded-full focus-visible:ring-3 focus-visible:ring-ring/50 focus-visible:outline-none">
          <Wordmark />
        </Link>

        <nav aria-label="Main" className="ml-2 hidden items-center gap-1 lg:flex">
          {nav.map((item) => (
            <NavLink key={item.label} {...item} />
          ))}
        </nav>

        <div className="ml-auto flex items-center gap-2">
          {IS_MOCK && (
            <span className="hidden rounded-full border border-dashed px-2.5 py-0.5 font-mono text-[0.7rem] text-muted-foreground lg:inline">
              mock data
            </span>
          )}
          <LiveIndicator />
          <TenantSwitcher tenant={tenant} />
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                variant="ghost"
                size="icon"
                onClick={toggle}
                aria-label={theme === "dark" ? "Switch to light theme" : "Switch to dark theme"}
              >
                {theme === "dark" ? <SunIcon /> : <MoonIcon />}
              </Button>
            </TooltipTrigger>
            <TooltipContent>{theme === "dark" ? "Light theme" : "Dark theme"}</TooltipContent>
          </Tooltip>
          <Tooltip>
            <TooltipTrigger asChild>
              <Button variant="ghost" size="icon" onClick={() => clearToken("user")} aria-label="Sign out">
                <LogOutIcon />
              </Button>
            </TooltipTrigger>
            <TooltipContent>Sign out</TooltipContent>
          </Tooltip>
        </div>
      </div>
      <nav aria-label="Main" className="no-scrollbar flex gap-1 overflow-x-auto px-4 pb-2 lg:hidden">
        {nav.map((item) => (
          <NavLink key={item.label} {...item} />
        ))}
      </nav>
    </header>
  );
}

function NavLink({ label, href, active }: { label: string; href: string; active: boolean }) {
  return (
    <Link
      href={href}
      aria-current={active ? "page" : undefined}
      className={cn(
        "rounded-full px-3.5 py-1.5 text-sm font-medium text-muted-foreground transition-colors outline-none hover:bg-accent hover:text-foreground focus-visible:ring-3 focus-visible:ring-ring/50",
        active && "bg-primary text-primary-foreground hover:bg-primary hover:text-primary-foreground",
      )}
    >
      {label}
    </Link>
  );
}

function LiveIndicator() {
  const status = useLiveStatus();
  const label = status === "live" ? "Live" : status === "connecting" ? "Connecting" : "Reconnecting";
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span
          role="status"
          tabIndex={0}
          className="hidden items-center gap-2 rounded-full px-2.5 py-1 text-xs font-medium text-muted-foreground outline-none focus-visible:ring-3 focus-visible:ring-ring/50 sm:inline-flex"
        >
          <span className="relative flex size-2">
            {status === "live" && (
              <span className="absolute inline-flex size-full animate-ping rounded-full bg-brand opacity-50 [animation-duration:2.4s]" />
            )}
            <span
              className={cn(
                "relative inline-flex size-2 rounded-full",
                status === "live" ? "bg-brand" : status === "connecting" ? "bg-muted-foreground" : "bg-amber",
              )}
            />
          </span>
          {label}
        </span>
      </TooltipTrigger>
      <TooltipContent>
        {status === "live"
          ? "Receiving live updates from the server"
          : status === "connecting"
            ? "Opening the live event stream"
            : "Live updates interrupted; reconnecting"}
      </TooltipContent>
    </Tooltip>
  );
}
