import { delta, fixed, int, pct } from "@/lib/format";
import type { Insight } from "@/lib/schemas";

type Row = Insight["data"]["rows"][number];
const num = (row: Row | undefined, key: string): number => Number(row?.[key] ?? NaN);
const str = (row: Row | undefined, key: string): string => String(row?.[key] ?? "");
const maxBy = (rows: Row[], key: string) => rows.reduce((a, b) => (num(b, key) > num(a, key) ? b : a));

/**
 * The one-line takeaway written on each insight, computed from that insight's own rows, so the note can never
 * drift from the data. Insights without a rule fall back to their question.
 */
const RULES: Record<string, (rows: Row[]) => string> = {
  "hourly-demand": (rows) => {
    const work = maxBy(rows.filter((r) => str(r, "day_type") === "working day"), "avg_trips");
    const rest = maxBy(rows.filter((r) => str(r, "day_type") !== "working day"), "avg_trips");
    return `Working days peak at ${num(work, "hour")}:00 (${int(num(work, "avg_trips"))} trips); weekends peak later, at ${num(rest, "hour")}:00.`;
  },
  "member-vs-casual": (rows) => {
    const [m, c] = [rows.find((r) => str(r, "member_casual") === "member"), rows.find((r) => str(r, "member_casual") === "casual")];
    return `Members take ${pct(num(m, "share_pct"), 1)} of trips; casual riders ride longer (median ${fixed(num(c, "median_minutes"), 1)} vs ${fixed(num(m, "median_minutes"), 1)} min).`;
  },
  "top-departure-stations": (rows) =>
    `${str(rows[0], "name")} leads with ${int(num(rows[0], "departures"))} departures, about ${int(num(rows[0], "per_day"))} a day.`,
  "top-routes": (rows) =>
    `The busiest route is ${str(rows[0], "from_station")} → ${str(rows[0], "to_station")}: ${int(num(rows[0], "trips"))} trips of ${fixed(num(rows[0], "median_minutes"), 1)} min.`,
  "round-trip-stations": (rows) =>
    `At ${str(rows[0], "name")}, ${pct(num(rows[0], "round_trip_pct"), 1)} of rides end where they began: riding for fun.`,
  "rain-impact": (rows) => {
    const worst = rows.reduce((a, b) => (num(b, "vs_dry_pct") < num(a, "vs_dry_pct") ? b : a));
    return `${str(worst, "rain")}: ${delta(num(worst, "vs_dry_pct"))} trips per hour compared with dry hours.`;
  },
  "temperature-bands": (rows) => {
    const best = maxBy(rows.filter((r) => num(r, "hours") >= 50), "avg_trips_per_hour");
    return `Among bands with at least 50 hours, riding peaks around ${num(best, "temp_band_c")} °C (${int(num(best, "avg_trips_per_hour"))} trips an hour).`;
  },
  "weekly-ebike-share": (rows) =>
    `E-bikes went from ${pct(num(rows[0], "ebike_pct"), 1)} to ${pct(num(rows.at(-1), "ebike_pct"), 1)} of weekly trips.`,
  "station-imbalance": (rows) => {
    const drain = rows.reduce((a, b) => (num(b, "net_per_day") < num(a, "net_per_day") ? b : a));
    const fill = maxBy(rows, "net_per_day");
    return `${str(drain, "name")} loses ${fixed(Math.abs(num(drain, "net_per_day")), 1)} bikes a day; ${str(fill, "name")} gains ${fixed(num(fill, "net_per_day"), 1)}.`;
  },
  "wildcard-search": (rows) =>
    rows.map((r) => `${int(num(r, "stations"))} stations match ${str(r, "pattern").replace(/^name /, "")}`).slice(0, 2).join("; ") + ".",
  "duration-histogram": (rows) => {
    const mode = maxBy(rows, "trips");
    return `The most common ride lasts ${num(mode, "minutes_from")}–${num(mode, "minutes_from") + 5} min (${pct(num(mode, "share_pct"), 1)} of trips).`;
  },
  "busiest-hour-per-station": (rows) =>
    `${str(rows[0], "name")} is busiest at ${num(rows[0], "busiest_hour")}:00, with ${pct(num(rows[0], "share_of_station_pct"), 1)} of its departures in that hour.`,
  "week-over-week": (rows) => {
    const changes = rows.filter((r) => r.wow_change_pct !== null);
    const up = maxBy(changes, "wow_change_pct");
    return `The biggest weekly jump was ${delta(num(up, "wow_change_pct"), 1)}, the week of ${str(up, "week_start")}.`;
  },
  "ebike-vs-classic-speed": (rows) => {
    const e = rows.find((r) => str(r, "rideable_type") === "electric_bike" && str(r, "member_casual") === "member");
    const c = rows.find((r) => str(r, "rideable_type") === "classic_bike" && str(r, "member_casual") === "member");
    return `Members ride e-bikes at a median ${fixed(num(e, "median_kmh"), 1)} km/h, classic bikes at ${fixed(num(c, "median_kmh"), 1)} km/h.`;
  },
};

export function finding(insight: Insight["data"]): string {
  const rule = RULES[insight.id];
  if (!rule || insight.rows.length === 0) return insight.question;
  return rule(insight.rows);
}
