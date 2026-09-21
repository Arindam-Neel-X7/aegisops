"use client";

import React from "react";
import { ThemeProvider } from "../providers/theme-provider";

/**
 * Minimal structural boundary for provider composition.
 * Providers for Theme (Step 5) and State/API (Step 6) will be composed here.
 */
export function Providers({ children }: { children: React.ReactNode }) {
  return (
    <ThemeProvider>
      {children}
    </ThemeProvider>
  );
}
