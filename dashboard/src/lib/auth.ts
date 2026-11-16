import { useSyncExternalStore } from "react";

// The admin token lives in localStorage so a reload keeps you signed in; "Sign out" removes it.
const STORAGE_KEY = "rillkit.admin_token";

type SignOutReason = "user" | "expired";
let lastSignOutReason: SignOutReason | null = null;
const listeners = new Set<() => void>();

function read(): string | null {
  try {
    return window.localStorage.getItem(STORAGE_KEY);
  } catch {
    return null;
  }
}

let current = read();

function emit() {
  for (const l of listeners) l();
}

export function getToken(): string | null {
  return current;
}

export function setToken(token: string) {
  current = token;
  lastSignOutReason = null;
  try {
    window.localStorage.setItem(STORAGE_KEY, token);
  } catch {
    // Private mode: the token lasts for this tab only.
  }
  emit();
}

export function clearToken(reason: SignOutReason = "user") {
  if (current === null) return;
  current = null;
  lastSignOutReason = reason;
  try {
    window.localStorage.removeItem(STORAGE_KEY);
  } catch {
    // Nothing stored.
  }
  emit();
}

export function consumeSignOutReason(): SignOutReason | null {
  const r = lastSignOutReason;
  lastSignOutReason = null;
  return r;
}

function subscribe(cb: () => void) {
  listeners.add(cb);
  const onStorage = (e: StorageEvent) => {
    if (e.key === STORAGE_KEY) {
      current = read();
      cb();
    }
  };
  window.addEventListener("storage", onStorage);
  return () => {
    listeners.delete(cb);
    window.removeEventListener("storage", onStorage);
  };
}

export function useToken(): string | null {
  return useSyncExternalStore(subscribe, getToken);
}
