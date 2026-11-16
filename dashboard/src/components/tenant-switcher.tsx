import { useState } from "react";
import { ChevronsUpDownIcon, PlusIcon, ShieldCheckIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { NewTenantDialog } from "@/components/new-tenant-dialog";
import { useTenants } from "@/api/queries";
import { navigate, paths } from "@/lib/router";

export function TenantSwitcher({ tenant }: { tenant: string }) {
  const { data: tenants } = useTenants();
  const [creating, setCreating] = useState(false);
  const current = tenants?.find((t) => t.id === tenant);

  return (
    <>
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="outline" className="max-w-56 gap-2" aria-label={`Tenant: ${current?.name ?? tenant}. Switch tenant`}>
            <span className="truncate">{current?.name ?? tenant}</span>
            <span className="hidden font-mono text-xs text-muted-foreground sm:inline">{tenant}</span>
            <ChevronsUpDownIcon data-icon="inline-end" className="text-muted-foreground" />
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" className="w-72">
          <DropdownMenuLabel>Tenants</DropdownMenuLabel>
          <DropdownMenuRadioGroup value={tenant} onValueChange={(id) => navigate(paths.overview(id))}>
            {tenants?.map((t) => (
              <DropdownMenuRadioItem key={t.id} value={t.id} className="py-2">
                <div className="flex min-w-0 flex-1 flex-col">
                  <span className="flex items-center gap-1.5 truncate">
                    {t.name}
                    {t.enforce_keys && <ShieldCheckIcon aria-label="Cloud mode" className="text-brand" />}
                  </span>
                  <span className="font-mono text-xs text-muted-foreground">
                    {t.id} · {t.nodes_online} online
                  </span>
                </div>
              </DropdownMenuRadioItem>
            ))}
          </DropdownMenuRadioGroup>
          <DropdownMenuSeparator />
          <DropdownMenuGroup>
            <DropdownMenuItem onSelect={() => setCreating(true)}>
              <PlusIcon />
              New tenant
            </DropdownMenuItem>
          </DropdownMenuGroup>
        </DropdownMenuContent>
      </DropdownMenu>
      <NewTenantDialog open={creating} onOpenChange={setCreating} />
    </>
  );
}
