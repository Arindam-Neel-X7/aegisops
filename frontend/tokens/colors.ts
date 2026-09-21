// Raw Palette (Not used directly by components)
export const rawPalette = {
  slate900: '#0B0E14',
  slate800: '#1E293B',
  slate700: '#334155',
  slate600: '#475569',
  slate500: '#64748B',
  slate400: '#94A3B8',
  slate300: '#CBD5E1',
  slate200: '#E2E8F0',
  slate100: '#F1F5F9',
  slate50: '#F8FAFC',
  white: '#FFFFFF',

  red600: '#DC2626',
  red500: '#EF4444',
  red400: '#F87171',
  red50: '#FEF2F2',

  amber600: '#D97706',
  amber500: '#F59E0B',
  amber400: '#FBBF24',
  amber50: '#FFFBEB',

  green600: '#16A34A',
  green500: '#22C55E',
  green400: '#4ADE80',
  green50: '#F0FDF4',

  blue600: '#2563EB',
  blue500: '#3B82F6',
  blue400: '#60A5FA',
  blue50: '#EFF6FF',

  purple600: '#9333EA',
  purple500: '#A855F7',
} as const;

export type SemanticColors = {
  // 1. Background
  'bg-base': string;
  'bg-surface': string;
  'bg-elevated': string;

  // 2. Text
  'text-primary': string;
  'text-secondary': string;
  'text-muted': string;

  // 3. Borders / dividers
  'border-subtle': string;
  'border-strong': string;

  // 4. Accent / interactive states
  'accent': string;
  'hover': string;
  'active': string;
  'focus': string;
  'disabled': string;

  // 5. Generic semantic states
  'state-critical': string;
  'state-warning': string;
  'state-success': string;
  'state-info': string;

  // 6. Incident severity
  'severity-critical': string;
  'severity-high': string;
  'severity-medium': string;
  'severity-low': string;

  // 7. Anomaly status
  'anomaly-detected': string;
  'anomaly-investigating': string;
  'anomaly-resolved': string;

  // 8. RCA confidence
  'rca-high': string;
  'rca-medium': string;
  'rca-low': string;

  // 9. Remediation risk
  'risk-high': string;
  'risk-medium': string;
  'risk-low': string;

  // 10. Visualization / chart semantics
  'viz-primary': string;
  'viz-secondary': string;
  'viz-tertiary': string;
};

// Light theme implements semantic colors
export const lightThemeColors: SemanticColors = {
  // Background
  'bg-base': rawPalette.slate50,
  'bg-surface': rawPalette.white,
  'bg-elevated': rawPalette.white,

  // Text
  'text-primary': rawPalette.slate900,
  'text-secondary': rawPalette.slate600,
  'text-muted': rawPalette.slate400,

  // Borders
  'border-subtle': rawPalette.slate200,
  'border-strong': rawPalette.slate300,

  // Accent / interactive
  'accent': rawPalette.blue600,
  'hover': rawPalette.blue500,
  'active': rawPalette.blue400,
  'focus': rawPalette.blue400,
  'disabled': rawPalette.slate300,

  // States
  'state-critical': rawPalette.red600,
  'state-warning': rawPalette.amber600,
  'state-success': rawPalette.green600,
  'state-info': rawPalette.blue600,

  // Incident severity
  'severity-critical': rawPalette.red600,
  'severity-high': rawPalette.amber600,
  'severity-medium': rawPalette.blue600,
  'severity-low': rawPalette.slate500,

  // Anomaly status
  'anomaly-detected': rawPalette.red600,
  'anomaly-investigating': rawPalette.amber600,
  'anomaly-resolved': rawPalette.green600,

  // RCA confidence
  'rca-high': rawPalette.green600,
  'rca-medium': rawPalette.amber600,
  'rca-low': rawPalette.red600,

  // Remediation risk
  'risk-high': rawPalette.red600,
  'risk-medium': rawPalette.amber600,
  'risk-low': rawPalette.green600,

  // Visualization
  'viz-primary': rawPalette.blue600,
  'viz-secondary': rawPalette.purple600,
  'viz-tertiary': rawPalette.green600,
};

// Dark theme implements exactly the SAME semantic keys
export const darkThemeColors: SemanticColors = {
  // Background
  'bg-base': rawPalette.slate900,
  'bg-surface': rawPalette.slate800,
  'bg-elevated': rawPalette.slate700,

  // Text
  'text-primary': rawPalette.slate50,
  'text-secondary': rawPalette.slate300,
  'text-muted': rawPalette.slate400,

  // Borders
  'border-subtle': rawPalette.slate700,
  'border-strong': rawPalette.slate600,

  // Accent / interactive
  'accent': rawPalette.blue500,
  'hover': rawPalette.blue400,
  'active': rawPalette.blue600,
  'focus': rawPalette.blue400,
  'disabled': rawPalette.slate600,

  // States
  'state-critical': rawPalette.red500,
  'state-warning': rawPalette.amber500,
  'state-success': rawPalette.green500,
  'state-info': rawPalette.blue500,

  // Incident severity
  'severity-critical': rawPalette.red500,
  'severity-high': rawPalette.amber500,
  'severity-medium': rawPalette.blue500,
  'severity-low': rawPalette.slate400,

  // Anomaly status
  'anomaly-detected': rawPalette.red500,
  'anomaly-investigating': rawPalette.amber500,
  'anomaly-resolved': rawPalette.green500,

  // RCA confidence
  'rca-high': rawPalette.green500,
  'rca-medium': rawPalette.amber500,
  'rca-low': rawPalette.red500,

  // Remediation risk
  'risk-high': rawPalette.red500,
  'risk-medium': rawPalette.amber500,
  'risk-low': rawPalette.green500,

  // Visualization
  'viz-primary': rawPalette.blue500,
  'viz-secondary': rawPalette.purple500,
  'viz-tertiary': rawPalette.green500,
};

// Extract token names for compile-time safety
export type ColorToken = keyof SemanticColors;
