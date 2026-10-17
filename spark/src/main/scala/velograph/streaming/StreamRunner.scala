package velograph.streaming

import java.nio.file.{Files, Path}
import java.util.Comparator

import scala.util.Try

import org.apache.spark.sql.streaming.StreamingQuery

/** How the streaming jobs start fresh and stop: a stop file, a time limit, or a failed query. */
object StreamRunner {

  /** `--fresh`: forget a checkpoint, so the queries start again from the latest offsets. */
  def deleteTree(dir: Path): Unit =
    if (Files.exists(dir)) {
      val stream = Files.walk(dir)
      try stream.sorted(Comparator.reverseOrder[Path]()).forEach(p => Files.delete(p))
      finally stream.close()
    }

  /**
   * Blocks until the stop file appears, `durationMin` minutes pass, or a query stops. Then it stops them all,
   * rethrows the first query's error if there was one, and says why it stopped. A stop never loses data: each
   * query resumes from its checkpoint on the next start.
   */
  def awaitStop(queries: Seq[StreamingQuery], stopFile: Path, durationMin: Option[Int]): String = {
    Files.deleteIfExists(stopFile)
    val deadline               = durationMin.map(m => System.currentTimeMillis() + m * 60000L)
    def reason: Option[String] =
      if (Files.exists(stopFile)) Some("stop file")
      else if (deadline.exists(System.currentTimeMillis() >= _)) durationMin.map(m => s"$m minutes elapsed")
      else None
    while (reason.isEmpty && queries.forall(_.isActive)) queries.head.awaitTermination(5000)
    val why = reason.getOrElse("a query stopped")
    queries.foreach(q => Try(q.stop()))
    Files.deleteIfExists(stopFile)
    queries.flatMap(_.exception).headOption.foreach(e => throw e)
    why
  }
}
