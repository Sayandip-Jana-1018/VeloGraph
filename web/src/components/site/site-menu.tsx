"use client";

import Link from "next/link";
import { DropdownMenu } from "radix-ui";

/** The main links as a menu, for screens too narrow for the header's inline nav. */
export function SiteMenu({ items }: { items: { href: string; label: string }[] }) {
  return (
    <DropdownMenu.Root modal={false}>
      <DropdownMenu.Trigger
        className="clay inline-flex items-center gap-2 rounded-full px-3.5 py-1.5 text-xs font-semibold text-fg-2 hover:text-marker lg:hidden"
        aria-label="Menu"
      >
        <svg aria-hidden viewBox="0 0 16 16" className="size-3.5" fill="none" stroke="currentColor" strokeWidth={2} strokeLinecap="round">
          <path d="M2.5 4h11M2.5 8h11M2.5 12h11" />
        </svg>
        <span className="hidden sm:inline">Menu</span>
      </DropdownMenu.Trigger>
      <DropdownMenu.Portal>
        <DropdownMenu.Content
          sideOffset={10}
          align="end"
          className="glass z-[80] min-w-56 rounded-3xl p-2 shadow-2xl"
        >
          {items.map((item) => (
            <DropdownMenu.Item key={item.href} asChild>
              <Link
                href={item.href}
                className="block rounded-2xl px-4 py-2.5 text-sm font-semibold text-fg-2 outline-none data-[highlighted]:bg-white/8 data-[highlighted]:text-marker"
              >
                {item.label}
              </Link>
            </DropdownMenu.Item>
          ))}
        </DropdownMenu.Content>
      </DropdownMenu.Portal>
    </DropdownMenu.Root>
  );
}
