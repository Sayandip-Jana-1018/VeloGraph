package velograph.common

import java.nio.file.Path
import java.time.{Duration, Instant}

import scala.util.Try

import org.apache.spark.sql.SparkSession

/**
 * Outcome of one job run, written to `data/serving/jobs/<job>.json` for the pipeline timeline and the report.
 */
final case class JobReport(
    job: String,
    status: String,
    appId: String,
    master: String,
    mode: String,
    startedAt: String,
    endedAt: String,
    durationMs: Long,
    metrics: Map[String, Any],
    error: Option[String]
)

object JobReport {

  def of(
      job: String,
      spark: SparkSession,
      cfg: AppConfig,
      started: Instant,
      ended: Instant,
      outcome: Try[Map[String, Any]]
  ): JobReport =
    JobReport(
      job = job,
      status = if (outcome.isSuccess) "succeeded" else "failed",
      appId = spark.sparkContext.applicationId,
      master = spark.sparkContext.master,
      mode = cfg.mode,
      startedAt = started.toString,
      endedAt = ended.toString,
      durationMs = Duration.between(started, ended).toMillis,
      metrics = outcome.getOrElse(Map.empty),
      error = outcome.failed.toOption.map(e => s"${e.getClass.getName}: ${e.getMessage}")
    )

  def write(dir: Path, report: JobReport): Path =
    ServingJson.write(dir.resolve(s"${report.job}.json"), "job-report", report.job, report)
}
