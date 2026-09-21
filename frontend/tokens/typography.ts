export const typography = {
  fontFamilies: {
    sans: 'var(--font-sans)',
    mono: 'var(--font-mono)',
  },
  fontSizes: {
    xs: '0.75rem',
    sm: '0.875rem',
    base: '1rem',
    lg: '1.25rem',
    xl: '1.5rem',
  },
  fontWeights: {
    regular: 400,
    medium: 500,
    'semi-bold': 600,
    bold: 700,
  },
  numericSettings: {
    tabular: 'tabular-nums',
  }
} as const;

export type FontFamilyToken = keyof typeof typography.fontFamilies;
export type FontSizeToken = keyof typeof typography.fontSizes;
export type FontWeightToken = keyof typeof typography.fontWeights;
export type NumericSettingToken = keyof typeof typography.numericSettings;
