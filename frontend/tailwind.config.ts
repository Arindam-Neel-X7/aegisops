import type { Config } from "tailwindcss";
import plugin from "tailwindcss/plugin";
import { lightThemeColors, darkThemeColors } from "./tokens/colors";

const config: Config = {
  content: [
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  darkMode: "class",
  theme: {
    extend: {},
  },
  plugins: [
    plugin(function ({ addBase }) {
      const lightVariables = Object.fromEntries(
        Object.entries(lightThemeColors).map(([key, value]) => [`--${key}`, value])
      );

      const darkVariables = Object.fromEntries(
        Object.entries(darkThemeColors).map(([key, value]) => [`--${key}`, value])
      );

      addBase({
        ":root": lightVariables,
        ".dark": darkVariables,
      });
    }),
  ],
};

export default config;
