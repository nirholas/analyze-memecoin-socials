import type { Metadata, Viewport } from "next";
import { Inter } from "next/font/google";
import "./globals.css";

/**
 * Inter font configuration
 * - Variable font for optimal performance
 * - Includes all weights used in the design (400-700)
 * - display: swap prevents FOUT while maintaining performance
 */
const inter = Inter({
  subsets: ["latin"],
  display: "swap",
  variable: "--font-inter",
  weight: ["400", "500", "600", "700"],
});

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
  maximumScale: 5, // Allow zoom for accessibility
};

export const metadata: Metadata = {
  title: "Tweet-Price Correlation Analyzer",
  description:
    "Do founder tweets correlate with token price? Analyze the relationship between crypto project founders' tweets and their token's price action across multiple assets.",
  icons: {
    icon: [
      { url: '/favicon.ico', sizes: 'any' },
      { url: '/favicon.svg', type: 'image/svg+xml' },
    ],
  },
  openGraph: {
    title: "Tweet-Price Correlation Analyzer",
    description:
      "Do founder tweets correlate with token price? Interactive charts and statistical analysis for PUMP, JUP, HYPE, ASTER, and more.",
    type: "website",
    images: [{ url: '/og-image.png', width: 1200, height: 630 }],
  },
  twitter: {
    card: "summary_large_image",
    site: "@frankdegods",
    title: "Tweet-Price Correlation Analyzer",
    description:
      "Do founder tweets correlate with token price? Interactive charts and statistical analysis for PUMP, JUP, HYPE, ASTER, and more.",
    images: ['/og-image.png'],
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className={inter.variable}>
      <body className="antialiased">{children}</body>
    </html>
  );
}
