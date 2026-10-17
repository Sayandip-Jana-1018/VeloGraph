package velograph.ml

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import velograph.common.{JobContext, ServingJson, SourceRange, SparkJob}
import velograph.etl.{LakeSchemas, LakeTable, LakeTables}
import velograph.gold.{GoldConfig, RouteTransforms}
import velograph.graph.{GraphConfig, GraphFeatures}

final case class LiveArchetypeCount(archetype: Int, name: String, stations: Long)
final case class LiveFeatureReport(
    profile: SourceRange,
    live: SourceRange,
    leakage: LeakageCheck,
    stations: Long,
    withProfile: Long,
    profileRows: Long,
    strongMinTrips: Long,
    medianCapacity: Int,
    communitiesMatched: Int,
    communitiesNew: Int,
    archetypes: Seq[LiveArchetypeCount]
)

/**
 * Roadmap P5.3, preparation: the static features of live scoring. The champions learned from rows whose
 * station, graph, archetype and hour-of-week features came from the eight weeks before them, so a live row
 * gets the same from profile-l, the last eight weeks of trips (ARCHITECTURE §8.2):
 *   - GraphX runs on that window's routes, and its communities take the fit window's ids, the numbering the
 *     models learned;
 *   - the archetypes are the profile-a clusters applied to profile-l (StationArchetypesJob);
 *   - the profiles are hour-of-week means and spreads over the window.
 * Writes `gold/live_stations` (one row per station) and `gold/live_profiles` (station x weekday x hour),
 * which LiveStationScoringJob broadcast-joins onto every micro-batch, and `serving/live-features.json`.
 */
object LiveFeaturesJob extends SparkJob {

  val jobName = "live-features"

  def run(ctx: JobContext, args: Seq[String]): Map[String, Any] = {
    val spark = ctx.spark
    import spark.implicits._
    val lake     = new LakeTables(spark, ctx.paths)
    val ml       = MlConfig(ctx.cfg.raw)
    val graphCfg = GraphConfig(ctx.cfg.raw)
    val window   = ml.live.profile
    val panel    = lake.read(LakeTable.StationHour)
    val stations = lake.read(LakeTable.Stations)

    // The same check as FeatureBuildJob's, on the data: the live rows start after the last profile day read.
    val lastDay =
      panel.where(window.covers()).agg(max("date")).head().getDate(0).toLocalDate // bounded: 1 row
    val check =
      LeakageCheck("live", lastDay.toString, ml.live.rows.from.toString, lastDay.isBefore(ml.live.rows.from))
    require(check.passed, s"leakage: live features read $lastDay, live rows start ${ml.live.rows.from}")
    require(lastDay == window.to, s"the lake ends on $lastDay, before profile-l does (${window.to})")

    // ---- Graph features of the window, with the fit window's community ids.
    val strong = FeatureBuildJob.strongRule(graphCfg, GoldConfig.span(ctx.cfg.raw), window)
    val routes = RouteTransforms.edges(
      lake.read(LakeTable.SilverTrips).where(window.covers(col("start_date"))),
      lake.read(LakeTable.Calendar),
      GoldConfig.peaks(ctx.cfg.raw)
    )
    val graph          = GraphFeatures.compute(spark, stations, routes, graphCfg, strong)
    val fitCommunities = lake
      .read(LakeTable.MlFeatures)
      .where(col("split") === ml.fit.split)
      .select("station_id", "community")
      .distinct()
      .as[(String, Int)]
      .collect() // bounded: one row per station
      .toMap
    val liveCommunities =
      graph.select("station_id", "community").as[(String, Int)].collect().toMap // bounded: one per station
    val alignment = FeatureTransforms.alignCommunities(fitCommunities, liveCommunities)
    val known     = fitCommunities.values.toSet
    val matched   = alignment.count { case (from, to) => from > 0 && known(to) }
    val fresh     = alignment.count(_._1 > 0) - matched
    val aligned   = graph
      .join(broadcast(alignment.toSeq.toDF("community", "aligned")), Seq("community"), "left")
      .withColumn("community", coalesce(col("aligned"), lit(0)))
      .drop("aligned")

    // ---- One row per station: every station of the registry, since the live feed lists GBFS stations too.
    val median   = FeatureBuildJob.medianCapacity(stations)
    val archetyp = lake
      .read(LakeTable.Archetypes)
      .where(col("window") === window.name)
      .select("station_id", "archetype", "archetype_name")
    val live: DataFrame = FeatureBuildJob
      .stationAttributes(stations.where(col("in_trips") || col("in_gbfs")), median)
      .join(stations.select("station_id", "name"), "station_id")
      .join(archetyp, Seq("station_id"), "left")
      .join(aligned, Seq("station_id"), "left")
      .withColumn("archetype", coalesce(col("archetype"), lit(-1)))
      .withColumn("archetype_name", coalesce(col("archetype_name"), lit("No profile")))
      .na
      .fill(0.0, Seq("pagerank_w", "pr_am", "pr_pm", "pr_shift", "imbalance_ratio", "clustering_coef"))
      .na
      .fill(0L, Seq("in_strength", "out_strength"))
      .na
      .fill(0, Seq("community"))
      .select(LakeSchemas.GoldLiveStation.fields.map(f => col(f.name).cast(f.dataType)): _*)
    live.coalesce(1).write.mode(SaveMode.Overwrite).parquet(lake.uri(LakeTable.LiveStations))

    val profiles = FeatureTransforms
      .hourOfWeek(panel, window)
      .select(LakeSchemas.GoldLiveProfile.fields.map(f => col(f.name).cast(f.dataType)): _*)
    profiles.coalesce(2).write.mode(SaveMode.Overwrite).parquet(lake.uri(LakeTable.LiveProfiles))

    // ---- Report, from the written tables.
    val written     = lake.read(LakeTable.LiveStations).persist(StorageLevel.MEMORY_AND_DISK)
    val profileRows = lake.read(LakeTable.LiveProfiles)
    val withProfile = profileRows.select("station_id").distinct().count()
    val counts      = written
      .groupBy("archetype", "archetype_name")
      .agg(count(lit(1)).as("stations"))
      .orderBy("archetype")
      .as[(Int, String, Long)]
      .collect() // bounded: k + 1 rows
      .map { case (a, n, c) => LiveArchetypeCount(a, n, c) }
      .toSeq
    val report = LiveFeatureReport(
      profile = SourceRange(window.from.toString, window.to.toString),
      live = SourceRange(ml.live.rows.from.toString, ml.live.rows.to.toString),
      leakage = check,
      stations = written.count(),
      withProfile = withProfile,
      profileRows = profileRows.count(),
      strongMinTrips = strong.minTrips,
      medianCapacity = median,
      communitiesMatched = matched,
      communitiesNew = fresh,
      archetypes = counts
    )
    ServingJson.write(
      ctx.paths.serving.resolve("live-features.json"),
      "live-features",
      jobName,
      report,
      rows = Some(report.stations),
      source = Some(report.profile)
    )
    log.info(
      s"live features from $window: ${report.stations} stations (${report.withProfile} with a profile), " +
        s"$matched of ${matched + fresh} districts matched to the fit window's"
    )
    Map(
      "window"             -> window.toString,
      "stations"           -> report.stations,
      "withProfile"        -> withProfile,
      "profileRows"        -> report.profileRows,
      "strongMinTrips"     -> strong.minTrips,
      "communitiesMatched" -> matched,
      "communitiesNew"     -> fresh,
      "archetypes"         -> counts.map(c => s"${c.name} (${c.stations})").mkString(", "),
      "leakageCheck"       -> s"${check.lastProfileDay} < ${check.firstRowDay}"
    )
  }
}
