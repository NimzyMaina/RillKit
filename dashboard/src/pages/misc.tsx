import { useState } from "react";
import { CompassIcon, PlusIcon, WavesIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Empty, EmptyContent, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import { Spinner } from "@/components/ui/spinner";
import { NewTenantDialog } from "@/components/new-tenant-dialog";
import { Wordmark } from "@/components/wordmark";
import { navigate } from "@/lib/router";
import { clearToken } from "@/lib/auth";

export function LoadingPage() {
  return (
    <div className="flex min-h-svh items-center justify-center" role="status" aria-label="Loading">
      <Spinner className="size-6 text-muted-foreground" />
    </div>
  );
}

export function NotFoundPage({ message }: { message?: string }) {
  return (
    <div className="flex min-h-svh flex-col items-center justify-center gap-10 p-6">
      <Wordmark />
      <Empty>
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <CompassIcon />
          </EmptyMedia>
          <EmptyTitle className="font-serif text-3xl font-normal">Nothing here</EmptyTitle>
          <EmptyDescription>{message ?? "This page does not exist."}</EmptyDescription>
        </EmptyHeader>
        <EmptyContent>
          <Button onClick={() => navigate("/")}>Go to the dashboard</Button>
        </EmptyContent>
      </Empty>
    </div>
  );
}

export function NoTenantsPage() {
  const [open, setOpen] = useState(false);
  return (
    <div className="flex min-h-svh flex-col items-center justify-center gap-10 p-6">
      <Wordmark />
      <Empty>
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <WavesIcon />
          </EmptyMedia>
          <EmptyTitle className="font-serif text-3xl font-normal">No tenants yet</EmptyTitle>
          <EmptyDescription>Create a tenant, then connect workers to it with an API key.</EmptyDescription>
        </EmptyHeader>
        <EmptyContent className="flex-row justify-center">
          <Button onClick={() => setOpen(true)}>
            <PlusIcon data-icon="inline-start" />
            New tenant
          </Button>
          <Button variant="ghost" onClick={() => clearToken("user")}>
            Sign out
          </Button>
        </EmptyContent>
      </Empty>
      <NewTenantDialog open={open} onOpenChange={setOpen} />
    </div>
  );
}
