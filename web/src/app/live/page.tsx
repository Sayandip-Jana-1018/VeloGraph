import type { Metadata } from "next";
import Link from "next/link";

import { Chip } from "@/components/inkglass/surfaces";
import { getLiveFeatures, getLiveSnapshot } from "@/lib/data";
import { int } from "@/lib/format";
import { LiveBoard } from "@/portals/streaming/live-board";

export const metadata: Metadata = {
  title: "Live",
  description:
    "Every Citi Bike station scored every 30 seconds by Spark Structured Streaming: what runs empty or full within the hour, and the van moves that fix it.",
};

/**
 * The operations page (roadmap P6.5). It renders the snapshot the last streaming run recorded, so it works with
 * nothing running; with the gateway up it follows the live stream.
 */
export default async function LivePage() {
  const [{ data: snapshot }, { data: features }] = await Promise.all([getLiveSnapshot(), getLiveFeatures()]);
  return (
    <div className="mx-auto max-w-[1280px] px-3 pt-12 pb-20 sm:px-6">
      <header className="mx-auto max-w-3xl text-center">
        <div className="flex flex-wrap justify-center gap-2">
          <Chip tone="unit">Unit V · Kafka</Chip>
          <Chip tone="unit">Spark Structured Streaming</Chip>
          <Chip tone="co">CO4 · CO5 · CO6</Chip>
        </div>
        <h1 className="mt-5 font-display text-[clamp(2.4rem,5vw,4rem)] leading-[1.02] font-extrabold text-fg">
          The next hour, every 30 seconds
        </h1>
        <p className="mt-4 text-base leading-relaxed text-fg-2">
          The live Citi Bike feed flows through three Kafka brokers into Spark on the cluster. Each micro-batch
          forecasts the current and the next hour at all {int(snapshot.kpis.stations)} stations with the champion
          models, projects every station&apos;s bikes 60 minutes ahead, and flags the ones about to run empty or full.
          Its station features come from the last eight weeks of trips ({features.profile.from} to{" "}
          {features.profile.to}), so the models see nothing they were not trained to read.
        </p>
      </header>
      <div className="mt-10">
        <LiveBoard initial={snapshot} />
      </div>
      <p className="mt-8 text-center text-sm text-fg-3">
        How the stream works, with its micro-batches, watermarks and failure drills:{" "}
        <Link href="/portal/streaming" className="text-marker underline-offset-4 hover:underline">
          open the streaming portal
        </Link>
        .
      </p>
    </div>
  );
}
