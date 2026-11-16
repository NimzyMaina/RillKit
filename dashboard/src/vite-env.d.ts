/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** "1" replaces the HTTP client with the in-memory fake in src/api/mock.ts. */
  readonly VITE_MOCK?: string;
}
