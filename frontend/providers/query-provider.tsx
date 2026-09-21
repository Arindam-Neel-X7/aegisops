"use client";

import React, { useState } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

export function QueryProvider({ children }: { children: React.ReactNode }) {
  // Initialize QueryClient lazily to ensure isolation per request/session in SSR contexts,
  // avoiding a shared global mutable QueryClient on the server.
  const [queryClient] = useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: {
            staleTime: 60 * 1000, // 1 minute default stale time to avoid over-fetching
            refetchOnWindowFocus: false, // Opt-out of aggressive refetching on window focus
            retry: 1, // Only retry failed requests once by default
          },
        },
      })
  );

  return (
    <QueryClientProvider client={queryClient}>
      {children}
    </QueryClientProvider>
  );
}
