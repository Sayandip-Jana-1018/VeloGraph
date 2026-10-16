import { cacheLife } from "next/cache";
import { codeToHtml } from "shiki";

import { Notate } from "@/components/inkglass/ink-client";
import { KpiPuck } from "@/components/inkglass/surfaces";
import { Act, CodePanel, Ledger, VivaNote, WithNote } from "@/components/portal/blocks";
import { getInsightIndex, getInsights, getJobReports } from "@/lib/data";
import { duration, int } from "@/lib/format";
import { jobMetric } from "@/lib/jobs";

import { finding } from "./findings";
import { InsightGallery, type InsightView } from "./insight-gallery";

async function highlightSql(sql: string): Promise<string> {
  "use cache";
  cacheLife("max");
  return codeToHtml(sql, { lang: "sql", theme: "tokyo-night" });
}

export async function SqlPortal() {
  const [index, insights, jobs] = await Promise.all([getInsightIndex(), getInsights(), getJobReports()]);
  const views: InsightView[] = await Promise.all(
    insights.map(async ({ data }) => ({
      id: data.id,
      title: data.title,
      question: data.question,
      chart: data.chart,
      columns: data.columns,
      rows: data.rows,
      sqlHtml: await highlightSql(data.sql),
      finding: finding(data),
      elapsedMs: data.elapsedMs,
    })),
  );
  const totalMs = jobMetric(jobs, "insights", "totalMs");
  const slowest = index.data.reduce((a, b) => (b.elapsedMs > a.elapsedMs ? b : a));
  const wildcard = insights.find((i) => i.data.id === "wildcard-search")!.data;

  return (
    <>
      <Act
        n={1}
        title="Fourteen questions, one query each"
        lede={
          <>
            Every insight is a named <span className="font-mono text-fg">.sql</span> file run by Spark SQL against temp
            views of the lake: joins, GROUP BY, ORDER BY, window functions, CTEs and{" "}
            <Notate type="underline" color="var(--color-amber)">
              wildcard searches
            </Notate>
            . Pick one to see its chart, its finding and the exact SQL.
          </>
        }
      >
        <div className="flex flex-wrap justify-center gap-4">
          <KpiPuck label="Queries" value={String(index.data.length)} note="named .sql files" />
          <KpiPuck label="All of them" value={duration(totalMs)} note="on Jun–Sep 2026" />
          <KpiPuck label="Slowest" value={duration(slowest.elapsedMs)} note={slowest.title} />
          <KpiPuck label="LIKE '%Park%'" value={int(Number(wildcard.rows[0].stations))} note="stations matched" />
        </div>
        <InsightGallery insights={views} />
      </Act>

      <Act
        n={2}
        title="Where the queries run"
        lede="InsightsJob registers the lake tables as temporary views, then runs each file through spark.sql; Catalyst plans every query, and the result rows, the SQL text and a chart hint are written to insights/<id>.json."
      >
        <WithNote
          note={
            <VivaNote>
              A temp view gives a DataFrame a name that SQL can use. % in LIKE matches any run of characters, _ exactly
              one; RLIKE takes a regular expression. Every query here aggregates, so no result is larger than 500 rows.
            </VivaNote>
          }
        >
          <CodePanel id="sql-views" caption="DataFrames become SQL tables" />
        </WithNote>
      </Act>

      <Ledger
        rows={[
          { label: "Insight queries", value: String(index.data.length), path: "insights/index.json · data.length" },
          { label: "Total query time", value: duration(totalMs), path: "jobs/insights.json · metrics.totalMs" },
          { label: "Slowest query", value: `${slowest.id}, ${duration(slowest.elapsedMs)}`, path: "insights/index.json · elapsedMs" },
          { label: "Stations with \"Park\"", value: int(Number(wildcard.rows[0].stations)), path: "insights/wildcard-search.json · rows[0].stations" },
        ]}
      />
    </>
  );
}
