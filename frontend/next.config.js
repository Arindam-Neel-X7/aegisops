/** @type {import('next').NextConfig} */
const nextConfig = {
  // Enable React 18 strict mode for development-time checks
  // (double-renders to surface side-effect bugs).
  reactStrictMode: true,

  // Remove the X-Powered-By: Next.js header (basic security hygiene).
  poweredByHeader: false,
};

module.exports = nextConfig;
