// Standard 8px grid snapping
export const spacing = {
  xs: '4px',
  sm: '8px',
  md: '16px',
  lg: '24px',
  xl: '32px',
} as const;

export type SpacingToken = keyof typeof spacing;
