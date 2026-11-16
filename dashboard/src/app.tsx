import { lazy, Suspense, useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Toaster } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { useToken } from "@/lib/auth";
import { navigate, paths, useRoute } from "@/lib/router";
import { useTenants } from "@/api/queries";
import { LoginPage } from "@/pages/login";
import { AppShell } from "@/components/app-shell";
import { OverviewPage } from "@/pages/overview";
import { StreamsPage } from "@/pages/streams";
import { ActivityPage } from "@/pages/activity";
import { SettingsPage } from "@/pages/settings";
import { NoTenantsPage, NotFoundPage, LoadingPage } from "@/pages/misc";

// The stream page carries the charting library; load it on demand.
const StreamDetailPage = lazy(() =>
  import("@/pages/stream-detail").then((m) => ({ default: m.StreamDetailPage })),
);

const LAST_TENANT_KEY = "rillkit.last_tenant";

export function App() {
  const token = useToken();
  const qc = useQueryClient();

  // Signing out drops every cached response.
  useEffect(() => {
    if (!token) qc.clear();
  }, [token, qc]);

  return (
    <TooltipProvider delayDuration={300}>
      {token ? <SignedIn /> : <LoginPage />}
      <Toaster position="bottom-right" />
    </TooltipProvider>
  );
}

function SignedIn() {
  const route = useRoute();
  const tenants = useTenants();

  // "/" goes to the last tenant used, or the first one.
  useEffect(() => {
    if (route.name !== "home" || !tenants.data?.length) return;
    let last: string | null = null;
    try {
      last = window.localStorage.getItem(LAST_TENANT_KEY);
    } catch {
      last = null;
    }
    const pick = tenants.data.find((t) => t.id === last) ?? tenants.data[0];
    navigate(paths.overview(pick.id), { replace: true });
  }, [route.name, tenants.data]);

  const tenant = "tenant" in route ? route.tenant : null;
  useEffect(() => {
    if (!tenant) return;
    try {
      window.localStorage.setItem(LAST_TENANT_KEY, tenant);
    } catch {
      // ignore
    }
  }, [tenant]);

  if (tenants.isPending) return <LoadingPage />;
  if (route.name === "home") {
    return tenants.data?.length === 0 ? <NoTenantsPage /> : <LoadingPage />;
  }
  if (route.name === "not-found" || !tenant) return <NotFoundPage />;
  if (tenants.data && !tenants.data.some((t) => t.id === tenant)) {
    return <NotFoundPage message={`There is no tenant called “${tenant}”.`} />;
  }

  return (
    <AppShell tenant={tenant}>
      {route.name === "overview" && <OverviewPage tenant={tenant} />}
      {route.name === "streams" && <StreamsPage tenant={tenant} />}
      {route.name === "stream" && (
        <Suspense fallback={<LoadingPage />}>
          <StreamDetailPage key={`${route.stream}/${route.group}`} tenant={tenant} stream={route.stream} group={route.group} />
        </Suspense>
      )}
      {route.name === "activity" && <ActivityPage tenant={tenant} />}
      {route.name === "settings" && <SettingsPage tenant={tenant} />}
    </AppShell>
  );
}
