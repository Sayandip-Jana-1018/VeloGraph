"use client";

import { useState } from "react";

import { ClaySegmented } from "@/components/inkglass/clay";
import type { MlRegression } from "@/lib/schemas";

import { CalibrationChart, HourErrorChart, ImportanceChart, RegressionLeaderboard, ResidualChart } from "./ml-charts";

/** The regression results for one target at a time: leaderboard, error by hour, calibration, residuals, importances. */
export function RegressionExplorer({ targets }: { targets: MlRegression["data"]["targets"] }) {
  const names = Object.keys(targets);
  const [target, setTarget] = useState(names[0]);
  const data = targets[target];
  return (
    <div className="space-y-5">
      <div className="flex justify-center">
        <ClaySegmented label="Target" value={target} onChange={setTarget} options={names.map((n) => ({ value: n, label: n }))} />
      </div>
      <RegressionLeaderboard target={target} data={data} />
      <div className="grid grid-cols-1 gap-5 lg:grid-cols-2">
        <HourErrorChart target={target} data={data} />
        <CalibrationChart target={target} data={data} />
      </div>
      <div className="grid grid-cols-1 gap-5 lg:grid-cols-2">
        <ResidualChart target={target} data={data} />
        <ImportanceChart target={target} data={data} />
      </div>
    </div>
  );
}
