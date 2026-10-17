package velograph.common

import java.nio.file.{Files, Path, StandardCopyOption}
import java.time.Instant

import scala.collection.immutable.ListMap

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/** Inclusive date range of the data an export was computed from. */
final case class SourceRange(from: String, to: String)

/**
 * Writes the JSON files the website and the report read (ARCHITECTURE §10). Every file has the same envelope
 * — `meta` (schema, version, generation time, producing job, source range, row count) and `data` — and is
 * replaced atomically, so a reader never sees half a file.
 */
object ServingJson {

  private val mapper  = new ObjectMapper().registerModule(DefaultScalaModule)
  private val printer = mapper.writerWithDefaultPrettyPrinter()

  /** `pretty = false` writes one line, for large files the website polls (the live snapshot). */
  def write(
      target: Path,
      schema: String,
      job: String,
      data: Any,
      rows: Option[Long] = None,
      source: Option[SourceRange] = None,
      pretty: Boolean = true
  ): Path = {
    val meta = ListMap[String, Any](
      "schema"      -> s"velograph.$schema",
      "version"     -> 1,
      "generatedAt" -> Instant.now().toString,
      "job"         -> job
    ) ++ source.map(range => "source" -> range) ++ rows.map(count => "rows" -> count)

    Files.createDirectories(target.getParent)
    val tmp      = target.resolveSibling(s".${target.getFileName}.tmp")
    val envelope = ListMap[String, Any]("meta" -> meta, "data" -> data)
    if (pretty) printer.writeValue(tmp.toFile, envelope) else mapper.writeValue(tmp.toFile, envelope)
    Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
  }
}
