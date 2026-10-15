package velograph.common

import java.time.Instant

import scala.util.Try

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/** What a job receives: the session and its resolved configuration. */
final case class JobContext(spark: SparkSession, cfg: AppConfig) {
  def paths: LakePaths = cfg.paths
}

/**
 * Template for every VeloGraph Spark job: build the session, run the job, then always write a job report
 * (also when the job fails) before stopping Spark. A failure is re-thrown so spark-submit exits non-zero.
 */
trait SparkJob {

  /** Stable kebab-case id, used in the Spark app name and as the job report file name. */
  def jobName: String

  /** Does the work and returns the metrics to record in the job report. */
  def run(ctx: JobContext, args: Seq[String]): Map[String, Any]

  protected lazy val log: Logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  final def main(args: Array[String]): Unit = {
    val cfg   = AppConfig.load()
    val spark = SparkSessionFactory.create(s"velograph-$jobName", cfg)
    log.info(s"$jobName starting: mode=${cfg.mode} master=${spark.sparkContext.master} " +
      s"app=${spark.sparkContext.applicationId} data=${cfg.paths.root}")

    val started = Instant.now()
    val outcome = Try(run(JobContext(spark, cfg), args.toSeq))
    val report  = JobReport.of(jobName, spark, cfg, started, Instant.now(), outcome)
    val path    = JobReport.write(cfg.paths.serving.resolve("jobs"), report)
    log.info(s"$jobName ${report.status} in ${report.durationMs} ms; report written to $path")

    spark.stop()
    outcome.failed.foreach(e => throw e)
  }
}
