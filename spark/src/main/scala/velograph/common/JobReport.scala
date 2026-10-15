package velograph.common

import java.nio.file.{Files, Path, StandardCopyOption}
import java.time.{Duration, Instant}

import scala.util.Try

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
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

  private val mapper =
    new ObjectMapper().registerModule(DefaultScalaModule).enable(SerializationFeature.INDENT_OUTPUT)

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

  /** Writes atomically (temp file, then rename) so a reader never sees half a report. */
  def write(dir: Path, report: JobReport): Path = {
    Files.createDirectories(dir)
    val meta: Map[String, Any] = Map(
      "schema"      -> "velograph.job-report",
      "version"     -> 1,
      "generatedAt" -> report.endedAt,
      "job"         -> report.job
    )
    val envelope: Map[String, Any] = Map("meta" -> meta, "data" -> report)
    val target                     = dir.resolve(s"${report.job}.json")
    val tmp                        = dir.resolve(s".${report.job}.json.tmp")
    mapper.writeValue(tmp.toFile, envelope)
    Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
  }
}
