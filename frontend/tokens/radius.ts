// Standard border radius tokens
export const radius = {
  sm: '4px',
  md: '6px',
  lg: '8px',
  xl: '12px',
  pill: '20px', // UI/UX explicit badge/pill radius
} as const;

export type RadiusToken = keyof typeof radius;
