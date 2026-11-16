import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import "./index.css";
import { loadApi } from "@/api";
import { setApi } from "@/api/queries";
import { ApiError } from "@/api/client";
import { applyTheme } from "@/lib/theme";
import { App } from "./app";

applyTheme();

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      retry: (count, err) => {
        // Client errors will not fix themselves.
        if (err instanceof ApiError && err.status >= 400 && err.status < 500) return false;
        return count < 2;
      },
    },
  },
});

void loadApi().then((api) => {
  setApi(api);
  createRoot(document.getElementById("root")!).render(
    <StrictMode>
      <QueryClientProvider client={queryClient}>
        <App />
      </QueryClientProvider>
    </StrictMode>,
  );
});
