"use client";

import { Slider, ToggleGroup } from "radix-ui";

import { cn } from "@/lib/cn";

/**
 * Clay: neumorphic controls. Soft extruded shadows show what can be pressed; every control still carries a text
 * label and a visible focus ring, because soft shadows alone do not meet contrast requirements.
 */

export function ClayButton({
  className,
  pressed,
  ...props
}: React.ButtonHTMLAttributes<HTMLButtonElement> & { pressed?: boolean }) {
  return (
    <button
      type="button"
      aria-pressed={pressed}
      className={cn(
        "inline-flex items-center justify-center gap-2 rounded-full px-4 py-2 text-sm font-semibold text-fg transition-[box-shadow,transform,color] duration-200 active:scale-[0.97] disabled:cursor-not-allowed disabled:opacity-40",
        pressed ? "clay-pressed text-marker" : "clay hover:text-marker",
        className,
      )}
      {...props}
    />
  );
}

export function ClaySegmented<T extends string>({
  label,
  value,
  options,
  onChange,
  className,
}: {
  label: string;
  value: T;
  options: { value: T; label: string }[];
  onChange: (value: T) => void;
  className?: string;
}) {
  return (
    <ToggleGroup.Root
      type="single"
      aria-label={label}
      value={value}
      onValueChange={(v) => v && onChange(v as T)}
      className={cn("clay-well inline-flex flex-wrap gap-1 rounded-full p-1", className)}
    >
      {options.map((o) => (
        <ToggleGroup.Item
          key={o.value}
          value={o.value}
          className="rounded-full px-3.5 py-1.5 text-xs font-semibold text-fg-2 transition-[box-shadow,color,background] duration-200 hover:text-fg data-[state=on]:clay data-[state=on]:text-marker"
        >
          {o.label}
        </ToggleGroup.Item>
      ))}
    </ToggleGroup.Root>
  );
}

export function ClaySlider({
  label,
  value,
  min,
  max,
  step = 1,
  onChange,
  className,
}: {
  label: string;
  value: number;
  min: number;
  max: number;
  step?: number;
  onChange: (value: number) => void;
  className?: string;
}) {
  return (
    <Slider.Root
      aria-label={label}
      value={[value]}
      min={min}
      max={max}
      step={step}
      onValueChange={([v]) => onChange(v)}
      className={cn("relative flex h-8 w-full touch-none items-center select-none", className)}
    >
      <Slider.Track className="clay-well relative h-3 grow overflow-hidden rounded-full">
        <Slider.Range className="absolute h-full rounded-full bg-gradient-to-r from-violet/70 to-marker/80" />
      </Slider.Track>
      <Slider.Thumb className="clay block size-6 rounded-full outline-none focus-visible:ring-2 focus-visible:ring-marker" />
    </Slider.Root>
  );
}
