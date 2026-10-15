package velograph.common

import java.nio.file.Path

/** Every location in the data lake, derived from a single root (ARCHITECTURE §5). */
final case class LakePaths(root: Path) {
  def raw: Path         = root.resolve("raw")
  def lake: Path        = root.resolve("lake")
  def bronze: Path      = lake.resolve("bronze")
  def silver: Path      = lake.resolve("silver")
  def gold: Path        = lake.resolve("gold")
  def graph: Path       = lake.resolve("graph")
  def models: Path      = root.resolve("models")
  def stream: Path      = root.resolve("stream")
  def checkpoints: Path = root.resolve("checkpoints")
  def serving: Path     = root.resolve("serving")
  def sparkEvents: Path = root.resolve("spark-events")
}

object LakePaths {

  /** Spark expects URIs rather than OS paths; `file:///C:/...` keeps Windows drive letters intact. */
  implicit final class PathOps(private val path: Path) extends AnyVal {
    def uri: String = path.toUri.toString
  }
}
