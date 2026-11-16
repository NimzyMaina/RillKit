import { useSyncExternalStore } from "react";

export type Theme = "light" | "dark";
const STORAGE_KEY = "rillkit.theme";
const listeners = new Set<() => void>();

function initial(): Theme {
  try {
    const stored = window.localStorage.getItem(STORAGE_KEY);
    if (stored === "light" || stored === "dark") return stored;
  } catch {
    // ignore
  }
  // The cream light theme is the default; dark is opt-in.
  return "light";
}

let theme: Theme = initial();

export function applyTheme(t: Theme = theme) {
  document.documentElement.classList.toggle("dark", t === "dark");
  document.documentElement.style.colorScheme = t;
}

export function setTheme(t: Theme) {
  theme = t;
  try {
    window.localStorage.setItem(STORAGE_KEY, t);
  } catch {
    // ignore
  }
  applyTheme(t);
  for (const l of listeners) l();
}

export function useTheme(): { theme: Theme; setTheme: (t: Theme) => void; toggle: () => void } {
  const t = useSyncExternalStore(
    (cb) => {
      listeners.add(cb);
      return () => listeners.delete(cb);
    },
    () => theme,
  );
  return { theme: t, setTheme, toggle: () => setTheme(t === "dark" ? "light" : "dark") };
}
