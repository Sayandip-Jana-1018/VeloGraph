import Link from "next/link";

import { Doodle } from "@/components/inkglass/ink";

import { ThemeSwitcher } from "./theme-switcher";

const NAV = [
  { href: "/#portals", label: "Portals" },
  { href: "/portal/network", label: "Network" },
  { href: "/portal/pregel", label: "Pregel" },
  { href: "/portal/spark-ui", label: "Spark UI" },
  { href: "/portal/models", label: "Models" },
  { href: "/live", label: "Live" },
];

export function SiteHeader() {
  return (
    <header className="sticky top-0 z-30 px-4 pt-4 sm:px-8">
      <div className="glass-frost mx-auto grid max-w-[1400px] grid-cols-[1fr_auto_1fr] items-center gap-4 rounded-full px-4 py-2.5 sm:px-6">
        <Link href="/" className="group flex items-center gap-2 justify-self-start" aria-label="VeloGraph home">
          <span className="font-display text-2xl leading-none font-extrabold tracking-tight text-fg">
            Velo<span className="text-marker">Graph</span>
          </span>
          <Doodle kind="spark" size={16} className="transition-transform duration-500 group-hover:rotate-90" />
        </Link>
        <nav aria-label="Main" className="hidden items-center gap-1 text-sm font-semibold md:flex">
          {NAV.map((item) => (
            <Link
              key={item.href}
              href={item.href}
              className="rounded-full px-4 py-1.5 text-fg-2 transition-colors hover:bg-white/5 hover:text-marker"
            >
              {item.label}
            </Link>
          ))}
        </nav>
        <div className="justify-self-end">
          <ThemeSwitcher />
        </div>
      </div>
    </header>
  );
}

export function SiteFooter() {
  return (
    <footer className="relative z-10 mx-auto mt-32 max-w-3xl px-5 pb-14 text-center text-xs leading-relaxed text-fg-3">
      <div className="mx-auto mb-8 h-px w-40 bg-gradient-to-r from-transparent via-marker/60 to-transparent" />
      <p className="font-display text-base font-bold text-fg-2">
        Velo<span className="text-marker">Graph</span> · INT315 Cluster Computing
      </p>
      <p className="mt-3">
        Trip data and GBFS feeds © NYC Bike Share, LLC, used under the Citi Bike Data License Agreement. VeloGraph is an
        independent student project, not affiliated with Citi Bike or Lyft. Weather by Open-Meteo.com (CC BY 4.0).
        Basemap © CARTO, © OpenStreetMap contributors.
      </p>
    </footer>
  );
}
