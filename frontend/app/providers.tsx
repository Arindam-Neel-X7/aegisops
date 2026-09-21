"use client";

import React from "react";

/**
 * Minimal structural boundary for provider composition.
 * Providers for Theme (Step 5) and State/API (Step 6) will be composed here.
 */
export function Providers({ children }: { children: React.ReactNode }) {
  return <>{children}</>;
}
