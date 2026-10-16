import type { Metadata } from "next";
import { Bricolage_Grotesque, Caveat, Inter, JetBrains_Mono } from "next/font/google";

import { Canvas } from "@/components/inkglass/canvas";
import { SiteFooter, SiteHeader } from "@/components/site/chrome";
import { InlineScript } from "@/components/site/inline-script";
import { DEFAULT_THEME, THEME_BOOT_SCRIPT } from "@/components/site/themes";

import "./globals.css";

// Self-hosted at build time by next/font, so the site needs no font requests (and works offline).
const display = Bricolage_Grotesque({ subsets: ["latin"], variable: "--font-bricolage", display: "swap" });
const sans = Inter({ subsets: ["latin"], variable: "--font-inter", display: "swap" });
const hand = Caveat({ subsets: ["latin"], variable: "--font-caveat", display: "swap" });
const mono = JetBrains_Mono({ subsets: ["latin"], variable: "--font-jetbrains", display: "swap" });

export const metadata: Metadata = {
  title: { default: "VeloGraph · real-time bike-share intelligence on Spark", template: "%s · VeloGraph" },
  description:
    "20.5 million Citi Bike trips through a Spark lake, Spark SQL, GraphX and MLlib, with a live feed on Kafka. An INT315 Cluster Computing project.",
};

export default function RootLayout({ children, portal }: LayoutProps<"/">) {
  return (
    <html
      lang="en"
      data-theme={DEFAULT_THEME}
      data-scroll-behavior="smooth" // lets Next.js turn smooth scrolling off during route changes
      suppressHydrationWarning // the boot script may switch data-theme before React hydrates
      className={`${display.variable} ${sans.variable} ${hand.variable} ${mono.variable}`}
    >
      <head>
        <InlineScript html={THEME_BOOT_SCRIPT} />
      </head>
      <body className="min-h-dvh">
        <Canvas />
        <SiteHeader />
        <main className="relative">{children}</main>
        <SiteFooter />
        {portal}
      </body>
    </html>
  );
}
