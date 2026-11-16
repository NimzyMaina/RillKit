import { useState, type FormEvent } from "react";
import { ArrowRightIcon, KeyRoundIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Field, FieldDescription, FieldError, FieldGroup, FieldLabel } from "@/components/ui/field";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Spinner } from "@/components/ui/spinner";
import { Wordmark } from "@/components/wordmark";
import { IS_MOCK } from "@/api";
import { ApiError } from "@/api/client";
import { api, errorTitle } from "@/api/queries";
import { consumeSignOutReason, setToken } from "@/lib/auth";

export function LoginPage() {
  const [token, setValue] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [expired] = useState(() => consumeSignOutReason() === "expired");

  async function submit(e: FormEvent) {
    e.preventDefault();
    const value = token.trim();
    if (!value) {
      setError("Paste the server's admin token.");
      return;
    }
    setBusy(true);
    setError(null);
    // Check the token before storing it.
    try {
      await api().checkToken(value);
      setToken(value);
    } catch (err) {
      setBusy(false);
      if (err instanceof ApiError && err.status === 401) setError("That token was rejected by the server.");
      else setError(`${errorTitle(err)}. ${err instanceof Error ? err.message : ""}`.trim());
    }
  }

  return (
    <main className="relative grid min-h-svh overflow-hidden lg:grid-cols-[1.15fr_1fr]">
      <div aria-hidden className="pointer-events-none absolute inset-0 overflow-hidden">
        <div className="absolute -top-40 -left-32 size-[34rem] rounded-full bg-lavender/60 blur-3xl dark:bg-lavender/10" />
        <div className="absolute top-1/2 left-1/3 size-[26rem] rounded-full bg-pink/40 blur-3xl dark:bg-pink/10" />
      </div>

      <section className="relative flex flex-col justify-between gap-16 p-8 md:p-14">
        <Wordmark size="lg" />
        <div className="flex max-w-xl flex-col gap-6">
          <h1 className="font-serif text-6xl leading-[0.98] font-normal tracking-tight text-balance md:text-7xl">
            Every stream, <em className="text-brand italic">gently</em> steered.
          </h1>
          <p className="max-w-md text-lg text-muted-foreground">
            Pause, drain and scale your Redis and Valkey Streams workers while they run. One control
            plane for every language.
          </p>
        </div>
        <p className="text-sm text-muted-foreground">RillKit control plane</p>
      </section>

      <section className="relative flex items-center justify-center p-6 md:p-14">
        <form
          onSubmit={submit}
          className="flex w-full max-w-md flex-col gap-6 rounded-[1.75rem] border bg-card p-8 md:p-10"
          aria-labelledby="signin-title"
        >
          <div className="flex flex-col gap-2">
            <h2 id="signin-title" className="font-serif text-4xl font-normal tracking-tight">
              Sign in
            </h2>
            <p className="text-muted-foreground">
              Use the admin token the server was started with (
              <code className="font-mono text-[0.85em]">RILLKIT_SERVER_ADMIN_TOKEN</code>).
            </p>
          </div>

          {expired && (
            <Alert>
              <KeyRoundIcon />
              <AlertTitle>Signed out</AlertTitle>
              <AlertDescription>The server no longer accepts the stored token. Sign in again.</AlertDescription>
            </Alert>
          )}

          <FieldGroup>
            <Field data-invalid={error ? true : undefined}>
              <FieldLabel htmlFor="token">Admin token</FieldLabel>
              <Input
                id="token"
                type="password"
                autoComplete="current-password"
                autoFocus
                value={token}
                onChange={(e) => setValue(e.target.value)}
                aria-invalid={error ? true : undefined}
                className="h-11 font-mono"
                placeholder="••••••••••••••••"
              />
              {error ? (
                <FieldError>{error}</FieldError>
              ) : (
                <FieldDescription>
                  Kept in this browser's local storage until you sign out.
                  {IS_MOCK && " Mock mode: any token works."}
                </FieldDescription>
              )}
            </Field>
          </FieldGroup>

          <Button type="submit" size="lg" disabled={busy}>
            {busy ? <Spinner data-icon="inline-start" /> : null}
            Continue
            {!busy && <ArrowRightIcon data-icon="inline-end" />}
          </Button>
        </form>
      </section>
    </main>
  );
}
