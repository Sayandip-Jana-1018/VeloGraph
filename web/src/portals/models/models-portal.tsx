import { Glass, KpiPuck } from "@/components/inkglass/surfaces";
import { Act, CodePanel, Ledger, VivaNote, WithNote } from "@/components/portal/blocks";
import {
  getArchetypes,
  getMlAblation,
  getMlClassification,
  getMlCorrelation,
  getMlFeatures,
  getMlRegression,
  getModelCards,
} from "@/lib/data";
import { delta, duration, fixed, int, pct } from "@/lib/format";

import {
  AblationChart,
  ArchetypeCurves,
  ArchetypeScatter,
  ClassificationLeaderboard,
  ConfusionMatrix,
  CorrelationHeatmap,
  SilhouetteChart,
  TopCorrelations,
} from "./ml-charts";
import { modelLabel } from "./labels";
import { RegressionExplorer } from "./regression-explorer";
import { WindowsTimeline } from "./windows-timeline";

const GROUP_LABEL: Record<string, string> = {
  calendar: "Calendar",
  weather: "Weather",
  station: "Station",
  graph: "GraphX",
  profile: "Hour-of-week profile",
};

export async function ModelsPortal() {
  const [{ data: arch }, { data: feat }, { data: corr }, { data: reg }, { data: cls }, { data: abl }, { data: cards }] =
    await Promise.all([
      getArchetypes(),
      getMlFeatures(),
      getMlCorrelation(),
      getMlRegression(),
      getMlClassification(),
      getMlAblation(),
      getModelCards(),
    ]);
  const fitStats = feat.splits.find((s) => s.split === "fit")!;
  const testStats = feat.splits.find((s) => s.split === "test")!;
  const groups = [...new Set(feat.features.map((f) => f.group))];
  const dep = reg.targets.departures;
  const depChampion = dep.leaderboard.find((r) => r.model === dep.champion)!;
  const naive = dep.baselines["seasonal-naive"].test;
  const clsChampion = cls.leaderboard.find((r) => r.model === cls.champion)!;
  const graphless = abl.variants.find((v) => v.dropped.length === 1 && v.dropped[0] === "graph");
  const profileless = abl.variants.find((v) => v.dropped.length === 1 && v.dropped[0] === "profile");
  const classNames = cls.classes.map((c) => c.name);
  const stable = 1 - arch.stability.changed / arch.stability.stations;

  return (
    <>
      <Act
        n={1}
        title="Three kinds of station"
        lede={`KMeans on the shape of every station's day (KMeans · Unit VI). The profile has 4 curves (departures and arrivals, on working days and days off) in 3-hour bins. Silhouette picked k = ${arch.k} from k = ${arch.selection[0].k}–${arch.selection.at(-1)!.k}.`}
      >
        <div className="flex flex-wrap justify-center gap-4">
          <KpiPuck label="Archetypes" count={{ to: arch.k }} note={`silhouette ${fixed(arch.silhouette, 3)}`} />
          <KpiPuck label="Stations clustered" count={{ to: arch.archetypes.reduce((s, a) => s + a.fitted, 0) }} note={`≥ ${int(arch.minTrips)} trips in the window`} />
          <KpiPuck label="Kept their type" value={pct(stable * 100, 0)} note="from the fit to the test window" />
        </div>
        <SilhouetteChart selection={arch.selection} k={arch.k} minShare={0.02} />
        <ArchetypeCurves archetypes={arch.archetypes} />
        <div className="grid grid-cols-1 gap-5 lg:grid-cols-[minmax(0,1fr)_22rem]">
          <ArchetypeScatter data={arch} />
          <div className="space-y-4">
            {arch.archetypes.map((a) => (
              <Glass key={a.id} variant="inset" className="p-4">
                <p className="font-display text-base font-bold text-fg">{a.name}</p>
                <p className="mt-0.5 text-xs text-fg-3">
                  working-day AM peak: {fixed(a.amDepartures, 1)} % leave · {fixed(a.amArrivals, 1)} % arrive · a day off is{" "}
                  {fixed(a.offToWork, 2)}× a working day
                </p>
                <p className="mt-2 text-sm text-fg-2">{a.examples.slice(0, 3).join(" · ")}</p>
              </Glass>
            ))}
          </div>
        </div>
      </Act>

      <Act
        n={2}
        title="Features that cannot see the future"
        lede={`${int(fitStats.rows)} August and ${int(testStats.rows)} September station-hours, with ${feat.features.length} features. Every profile, graph and archetype feature comes from the eight weeks before its rows, and the job checks that on the data it read.`}
      >
        <WindowsTimeline data={feat} />
        <WithNote
          note={
            <VivaNote>
              A model must not learn from the hours it is asked to predict. So the features of an August row come from
              June and July only, and those of a September row from July and August. The GraphX features are recomputed
              on those windows too; the P3 graph covers September and would leak.
            </VivaNote>
          }
        >
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
            {groups.map((g) => (
              <Glass key={g} variant="inset" className="p-4">
                <p className="font-display text-sm font-bold text-fg">{GROUP_LABEL[g] ?? g}</p>
                <ul className="mt-2 space-y-1 text-xs text-fg-2">
                  {feat.features
                    .filter((f) => f.group === g)
                    .map((f) => (
                      <li key={f.name}>
                        <span className="font-mono text-fg">{f.name}</span> · {f.description}
                      </li>
                    ))}
                </ul>
              </Glass>
            ))}
          </div>
        </WithNote>
        <Ledger
          rows={[
            ...feat.leakage.map((l) => ({
              label: `${l.split}: last feature day before the first row day`,
              value: `${l.lastProfileDay} < ${l.firstRowDay} ${l.passed ? "✓" : "✗"}`,
              path: "ml-features.json · leakage",
            })),
            { label: "Regime threshold τ (from the fit rows only)", value: `${fixed(feat.tau, 4)} × capacity`, path: "ml-features.json · tau" },
            {
              label: "Regime classes, fit split",
              value: `${pct((fitStats.draining / fitStats.rows) * 100)} draining · ${pct((fitStats.stable / fitStats.rows) * 100)} stable · ${pct((fitStats.filling / fitStats.rows) * 100)} filling`,
              path: "ml-features.json · splits",
            },
            { label: "Districts matched across the two windows", value: `${feat.communitiesMatched} of ${feat.communitiesMatched + feat.communitiesNew}`, path: "ml-features.json · communitiesMatched" },
          ]}
        />
        <CodePanel id="ml-leakage-check" caption="FeatureBuildJob checks the windows on the data itself" />
      </Act>

      <Act
        n={3}
        title="What moves with demand"
        lede={`A station-hour sees ${fixed(corr.dispersion.mean, 2)} departures on average, but ${pct(corr.dispersion.zeroShare * 100, 0)} of hours see none, and the variance is ${fixed(corr.dispersion.variance / corr.dispersion.mean, 0)}× the mean. The station's own recent history at that weekday and hour is the strongest signal (correlation heat map · Unit VI).`}
      >
        <div className="grid grid-cols-1 gap-5 lg:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)]">
          <TopCorrelations rows={corr.withDepartures} />
          <CorrelationHeatmap data={corr} />
        </div>
      </Act>

      <Act
        n={4}
        title="Forecasting the next hour"
        lede={`Linear regression, a decision tree, a random forest and gradient-boosted trees, each tuned on 1–24 August and chosen on the last August week. September is only reported. For departures, the ${modelLabel(dep.champion).toLowerCase()} has an MAE of ${fixed(depChampion.test.mae, 3)} against the seasonal naive forecast's ${fixed(naive.mae, 3)}: ${fixed(dep.improvementVsNaive, 1)} % lower (R² · RMSE · MAE).`}
      >
        <RegressionExplorer targets={reg.targets} />
        <WithNote
          note={
            <VivaNote>
              The seasonal naive forecast predicts a station&apos;s own average for that weekday and hour. Beating it means
              the model learned something beyond habit: weather, holidays, and what similar stations do. MAE is in
              trips per station-hour, so it reads directly.
            </VivaNote>
          }
        >
          <CodePanel id="ml-lab-pipeline" caption="one Pipeline per model in the PySpark lab" />
        </WithNote>
      </Act>

      <Act
        n={5}
        title="Will a station drain or fill?"
        lede={`Each hour is labelled draining, stable or filling against ${fixed(cls.tau, 3)} × capacity. ${pct(cls.balance.test.stable * 100, 0)} of hours are stable, so accuracy flatters; the models are chosen on macro-F1 and trained with class weights. The ${modelLabel(cls.champion).replace("Linear SVM", "linear SVM")} reaches a macro-F1 of ${fixed(clsChampion.test.macroF1, 3)} (confusion matrix · Unit VI).`}
      >
        <div className="grid grid-cols-1 gap-5 lg:grid-cols-[minmax(0,1.1fr)_minmax(0,0.9fr)]">
          <ClassificationLeaderboard leaderboard={cls.leaderboard} baselines={cls.baselines} champion={cls.champion} />
          <ConfusionMatrix metrics={clsChampion.test} names={classNames} title={`${modelLabel(cls.champion)}: September`} />
        </div>
        <Ledger
          rows={clsChampion.test.perClass.map((c) => ({
            label: `${classNames[c.class]}: precision · recall · F1`,
            value: `${fixed(c.precision, 3)} · ${fixed(c.recall, 3)} · ${fixed(c.f1, 3)}`,
            path: `ml-classification.json · leaderboard[${cls.champion}].test.perClass[${c.class}]`,
          }))}
        />
        <CodePanel id="ml-class-weights" caption="class weights: every class carries the same total weight" />
      </Act>

      <Act
        n={6}
        title="What the graph adds"
        lede={`The departures champion refit without whole feature groups. ${graphless ? `Without the GraphX features its September MAE changes by ${graphless.deltaMaePct >= 0 ? "+" : ""}${fixed(graphless.deltaMaePct, 1)} %` : ""}${profileless ? `, without the hour-of-week profiles by ${profileless.deltaMaePct >= 0 ? "+" : ""}${fixed(profileless.deltaMaePct, 1)} %` : ""}. That is CO6 measured inside CO4.`}
      >
        <AblationChart data={abl} />
      </Act>

      <Act
        n={7}
        title="Two implementations, one model"
        lede="The Scala TrainChampionJob retrains each champion with the lab's configuration and feature pipeline, and must reproduce the lab's September score within 1 %, or it fails. Those are the models the streaming job will load."
      >
        <Glass variant="inset" className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead>
              <tr className="border-b border-white/8 text-xs text-fg-3">
                <th className="px-4 py-3">Model</th>
                <th className="px-4 py-3">Algorithm</th>
                <th className="px-4 py-3 text-right">September (Scala)</th>
                <th className="px-4 py-3 text-right">Lab (PySpark)</th>
                <th className="px-4 py-3 text-right">Difference</th>
                <th className="px-4 py-3 text-right">Training</th>
              </tr>
            </thead>
            <tbody>
              {cards.map((c) => (
                <tr key={c.name} className="border-b border-white/5 last:border-0">
                  <td className="px-4 py-2.5 font-semibold text-fg">{c.name}</td>
                  <td className="px-4 py-2.5 text-fg-2">{modelLabel(c.algorithm)}</td>
                  <td className="px-4 py-2.5 text-right text-fg tabular-nums">
                    {c.metric === "mae" ? "MAE" : "macro-F1"} {fixed(c.testMetric, 4)}
                  </td>
                  <td className="px-4 py-2.5 text-right text-fg-2 tabular-nums">{fixed(c.labMetric, 4)}</td>
                  <td className="px-4 py-2.5 text-right text-fg-2 tabular-nums">{delta(c.deltaPct, 2)}</td>
                  <td className="px-4 py-2.5 text-right text-fg-3 tabular-nums">
                    {int(c.trainingRows)} rows · {duration(c.trainingMillis)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </Glass>
        <WithNote
          note={
            <VivaNote>
              The lab is where models are compared; the Scala job is what production runs. Rebuilding the same feature
              pipeline in Scala and matching the lab&apos;s score is the proof that they are the same model, and every
              saved model has a card with its data windows, parameters and git commit.
            </VivaNote>
          }
        >
          <CodePanel id="ml-champion-parity" caption="TrainChampionJob refuses a model that does not match the lab" />
        </WithNote>
        <CodePanel id="ml-kmeans-selection" caption="StationArchetypesJob: best of five KMeans runs per k, k by silhouette" />
      </Act>
    </>
  );
}
