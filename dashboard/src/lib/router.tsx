import { useSyncExternalStore, type AnchorHTMLAttributes, type MouseEvent } from "react";

// A tiny pathname router: history.pushState + popstate, read through a hook.

const NAVIGATE_EVENT = "rillkit:navigate";

function subscribe(cb: () => void) {
  window.addEventListener("popstate", cb);
  window.addEventListener(NAVIGATE_EVENT, cb);
  return () => {
    window.removeEventListener("popstate", cb);
    window.removeEventListener(NAVIGATE_EVENT, cb);
  };
}

const getPath = () => window.location.pathname;
const getSearch = () => window.location.search;

export function usePathname(): string {
  return useSyncExternalStore(subscribe, getPath);
}

/** One query parameter of the current url (null when absent), kept in sync with navigation. */
export function useSearchParam(name: string): string | null {
  const search = useSyncExternalStore(subscribe, getSearch);
  return new URLSearchParams(search).get(name);
}

/** Set (or with null, remove) a query parameter without adding a history entry. */
export function setSearchParam(name: string, value: string | null) {
  const params = new URLSearchParams(window.location.search);
  if (value === null) params.delete(name);
  else params.set(name, value);
  const qs = params.toString();
  navigate(`${window.location.pathname}${qs ? `?${qs}` : ""}`, { replace: true });
}

export function navigate(to: string, opts: { replace?: boolean } = {}) {
  if (to === window.location.pathname + window.location.search) return;
  if (opts.replace) window.history.replaceState(null, "", to);
  else window.history.pushState(null, "", to);
  window.dispatchEvent(new Event(NAVIGATE_EVENT));
  if (!opts.replace) window.scrollTo({ top: 0 });
}

export type Route =
  | { name: "home" }
  | { name: "overview"; tenant: string }
  | { name: "streams"; tenant: string }
  | { name: "stream"; tenant: string; stream: string; group: string }
  | { name: "activity"; tenant: string }
  | { name: "settings"; tenant: string }
  | { name: "not-found" };

const dec = (s: string) => {
  try {
    return decodeURIComponent(s);
  } catch {
    return s;
  }
};

export function matchRoute(pathname: string): Route {
  const parts = pathname.split("/").filter(Boolean).map(dec);
  if (parts.length === 0) return { name: "home" };
  if (parts[0] !== "t" || parts.length < 2) return { name: "not-found" };
  const tenant = parts[1];
  const rest = parts.slice(2);
  if (rest.length === 0) return { name: "overview", tenant };
  if (rest[0] === "streams" && rest.length === 1) return { name: "streams", tenant };
  if (rest[0] === "streams" && rest.length === 3)
    return { name: "stream", tenant, stream: rest[1], group: rest[2] };
  if (rest[0] === "activity" && rest.length === 1) return { name: "activity", tenant };
  if (rest[0] === "settings" && rest.length === 1) return { name: "settings", tenant };
  return { name: "not-found" };
}

export function useRoute(): Route {
  return matchRoute(usePathname());
}

const enc = encodeURIComponent;

export const paths = {
  overview: (t: string) => `/t/${enc(t)}`,
  streams: (t: string) => `/t/${enc(t)}/streams`,
  stream: (t: string, stream: string, group: string) =>
    `/t/${enc(t)}/streams/${enc(stream)}/${enc(group)}`,
  activity: (t: string) => `/t/${enc(t)}/activity`,
  settings: (t: string) => `/t/${enc(t)}/settings`,
};

/** An <a> that navigates without a page load (but still opens in a new tab with a modifier). */
export function Link({
  href,
  onClick,
  ...props
}: AnchorHTMLAttributes<HTMLAnchorElement> & { href: string }) {
  const handle = (e: MouseEvent<HTMLAnchorElement>) => {
    onClick?.(e);
    if (
      e.defaultPrevented ||
      e.button !== 0 ||
      e.metaKey ||
      e.ctrlKey ||
      e.shiftKey ||
      e.altKey ||
      props.target === "_blank"
    )
      return;
    e.preventDefault();
    navigate(href);
  };
  return <a href={href} onClick={handle} {...props} />;
}
