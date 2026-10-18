# Scala tour: Unit II in VeloGraph's own code

Unit II covers Scala's types, collections and functions. Every excerpt below is a real line of this repository, with
its file and line, so you can open it in the viva and say why it is written that way. Spark itself is written in
Scala, and every VeloGraph job uses Scala 2.12.

Paths are relative to `spark/src/main/scala/velograph/`.

## 1. Traits and objects

A **trait** is an interface that can also carry code. An **object** is a singleton: Scala has no `static`, so a
job's entry point and its helpers live in one.

[`common/SparkJob.scala:19`](../../spark/src/main/scala/velograph/common/SparkJob.scala#L19)

```scala
trait SparkJob {

  /** Stable kebab-case id, used in the Spark app name and as the job report file name. */
  def jobName: String

  /** Does the work and returns the metrics to record in the job report. */
  def run(ctx: JobContext, args: Seq[String]): Map[String, Any]
  ...
  final def main(args: Array[String]): Unit = {
```

**Why here:** every job gets the same `main`, a template method. It builds the session, runs the job, and always
writes a job report, even when the job fails. A job only fills in `jobName` and `run`. `final` stops a job from
overriding `main` by accident.

[`graph/WeightedPageRank.scala:27`](../../spark/src/main/scala/velograph/graph/WeightedPageRank.scala#L27)

```scala
object WeightedPageRank {
```

## 2. Case classes

A **case class** is an immutable record. It comes with equality by value, `copy`, `toString` and pattern matching
for free, and Spark can derive an encoder for it.

[`graph/WeightedPageRank.scala:33`](../../spark/src/main/scala/velograph/graph/WeightedPageRank.scala#L33)

```scala
  final case class State(rank: Double, delta: Double, superstep: Int, updates: BitSet)
```

**Why here:** each vertex's Pregel state is a value, never mutated. `vprog` returns a new `State`. `final` because
nothing should extend a record.

The same idea gives a typed Dataset. The import supplies an encoder for the case class `StationStatus`, which turns
rows into objects:
[`streaming/LiveStationScoringJob.scala:197`](../../spark/src/main/scala/velograph/streaming/LiveStationScoringJob.scala#L197)

```scala
    LiveTransforms.newest(kafkaRows).select(LiveTransforms.StatusColumns: _*).as[StationStatus].collect()
```

The excerpt's bound: one row per station, about 2,500.

## 3. `val`, immutability and `lazy val`

`val` is a constant reference, and `var` (rare here) is a variable. A `lazy val` is computed on first use, then kept.

[`common/SparkJob.scala:33`](../../spark/src/main/scala/velograph/common/SparkJob.scala#L33)

```scala
  protected lazy val log: Logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))
```

**Why here:** the logger is created only when a job first logs. `stripSuffix("$")` removes the `$` that the
compiler adds to an object's class name.

## 4. Functions are values; higher-order functions

A function can be stored in a `val` and passed to another function. Spark's whole API is higher-order functions.

[`graph/Communities.scala:40`](../../spark/src/main/scala/velograph/graph/Communities.scala#L40)

```scala
    val sendMsg = (t: EdgeTriplet[Label, Double]) =>
      Iterator(t.srcId -> Map(t.dstAttr.id -> t.attr), t.dstId -> Map(t.srcAttr.id -> t.attr))
    val mergeMsg = (a: Votes, b: Votes) =>
      b.foldLeft(a) { case (votes, (label, weight)) =>
        votes.updated(label, votes.getOrElse(label, 0.0) + weight)
      }

    // Every vertex with an edge hears from its neighbours in every superstep, so this runs exactly `supersteps`.
    Pregel(start, Map.empty[VertexId, Double], supersteps)(vprog, sendMsg, mergeMsg)
```

**Why here:** `Pregel` takes three functions (`vprog`, `sendMsg`, `mergeMsg`), and they are shipped to the
executors. `_ + _` is shorthand for `(a, b) => a + b`:
[`graph/WeightedPageRank.scala:49`](../../spark/src/main/scala/velograph/graph/WeightedPageRank.scala#L49)

```scala
      graph.aggregateMessages[Double](ctx => ctx.sendToSrc(ctx.attr), _ + _, TripletFields.EdgeOnly)
```

## 5. `Option` instead of `null`

`Option[A]` is either `Some(a)` or `None`. It makes "maybe missing" visible in the type, so the compiler forces you
to handle it.

[`rdd/CsvLine.scala:50`](../../spark/src/main/scala/velograph/rdd/CsvLine.scala#L50)

```scala
  def parseTrip(line: String): Option[TripLite] = {
    val f = split(line)
    if (f.length != 13 || f.head == "ride_id") None
    else
      Try(
        TripLite(
          rideId = f(0),
          ...
          endStationId = Some(f(7)).filter(_.nonEmpty).map(StationIds.normalize)
        )
      ).toOption.filter(_.startStationId.nonEmpty)
  }
```

**Why here:** a header line, a malformed line and a bad timestamp all become `None`, and the RDD lab keeps the
`Some`s with `flatMap`. A trip with no end station still parses, with `endStationId = None`.

## 6. `Try` instead of `try`/`catch`

`Try(expr)` is `Success(value)` or `Failure(exception)`: an error becomes a value you can pass around.

[`common/SparkJob.scala:42`](../../spark/src/main/scala/velograph/common/SparkJob.scala#L42)

```scala
    val outcome = Try(run(JobContext(spark, cfg), args.toSeq))
    val report  = JobReport.of(jobName, spark, cfg, started, Instant.now(), outcome)
    ...
    spark.stop()
    outcome.failed.foreach(e => throw e)
```

**Why here:** the job report needs the outcome either way: metrics on success, the error on failure. The exception is
re-thrown only after the report is written and Spark is stopped, so `spark-submit` still exits non-zero.

## 7. Pattern matching

`match` is a `switch` that can look inside values: constants, types, lists, tuples, options, with guards.

**On characters**: the CSV splitter,
[`rdd/CsvLine.scala:38`](../../spark/src/main/scala/velograph/rdd/CsvLine.scala#L38):

```scala
        c match {
          case '"'   => inQuotes = true
          case ','   => fields += field.result(); field.clear()
          case other => field += other
        }
```

**On lists**: `head :: tail` against `Nil`,
[`sql/InsightQueries.scala:60`](../../spark/src/main/scala/velograph/sql/InsightQueries.scala#L60):

```scala
  def parseChart(spec: String): Map[String, String] =
    spec.trim.split("\\s+").toList match {
      case kind :: options =>
        options.map { option =>
          option.split("=", 2) match {
            case Array(key, value) => key -> value
            case _ => throw new IllegalArgumentException(s"bad chart option '$option' in '$spec'")
          }
        }.toMap + ("type" -> kind)
      case Nil => Map.empty
    }
```

**With guards**: the same name picks a classifier or a regressor depending on the label,
[`ml/FeaturePipeline.scala:83`](../../spark/src/main/scala/velograph/ml/FeaturePipeline.scala#L83):

```scala
    algorithm match {
      case "linear-regression"                  => configured(new LinearRegression().setLabelCol(label))
      case "decision-tree" if label == "regime" =>
        weighted(configured(new DecisionTreeClassifier().setLabelCol(label)))
      case "decision-tree"                      => configured(new DecisionTreeRegressor().setLabelCol(label))
      ...
      case other => throw new IllegalArgumentException(s"unknown algorithm $other")
    }
```

**On types**: MLlib's `getParam` loses the parameter's type, and the `Param` subclass gives it back,
[`ml/FeaturePipeline.scala:68`](../../spark/src/main/scala/velograph/ml/FeaturePipeline.scala#L68):

```scala
        val value: Any = (param: Param[_]) match { // getParam erases the type; the Param subclass carries it
          case _: IntParam     => params.getInt(key)
          case _: LongParam    => params.getLong(key)
          case _: DoubleParam  => params.getDouble(key)
          case _: BooleanParam => params.getBoolean(key)
          case _               => params.getString(key)
        }
```

**On tuples and options, inside `collect`**: the data-quality report,
[`etl/DataQuality.scala:46`](../../spark/src/main/scala/velograph/etl/DataQuality.scala#L46):

```scala
    val validRows = reasonRows.collectFirst { case (None, rows) => rows }.getOrElse(0L)
    val byReason  = reasonRows
      .collect { case (Some(reason), rows) => ReasonCount(reason, rows, pct(rows, inputRows)) }
```

A `None` reason means a valid row and a `Some` reason a quarantined one. `collect` filters and maps in one step,
with a partial function.

**Binders and destructuring**: `pair @ (x, y)` names the whole tuple and its parts, and
`val ((a, b), gain) = ...` unpacks a result,
[`graph/Communities.scala:162`](../../spark/src/main/scala/velograph/graph/Communities.scala#L162):

```scala
        val gains = links.map { case (pair @ (x, y), w) =>
          pair -> (w / m - degree(x) * degree(y) / (2 * m * m))
        }
        val ((a, b), gain) = gains.maxBy { case ((x, y), g) => (g, -x, -y) } // ties: smallest labels first
```

## 8. Collections: `map`, `filter`, `foldLeft`, `collect`, `maxBy`

Scala's immutable collections share one API, and an operation returns a new collection.

[`graph/Communities.scala:60`](../../spark/src/main/scala/velograph/graph/Communities.scala#L60): who wins a
label-propagation vote.

```scala
  def winner(current: VertexId, votes: Votes): VertexId = {
    val best = votes.values.max // weights are whole trip counts, so equal sums compare exactly
    if (votes.get(current).contains(best)) current
    else votes.collect { case (label, weight) if weight == best => label }.min
  }
```

**Why here:** the tie rule is deterministic, so the same graph always gives the same districts. That is part of why
a pipeline rerun reproduces every number (Q94).

## 9. Tail recursion

`@tailrec` asks the compiler to prove that the recursive call is the last step, so it becomes a loop and cannot
overflow the stack. The build fails if it is not.

[`graph/Communities.scala:154`](../../spark/src/main/scala/velograph/graph/Communities.scala#L154): the greedy
modularity merge that turns 59 label-propagation communities into 15 districts.

```scala
    @tailrec
    def loop(
        links: Map[(VertexId, VertexId), Double],
        degree: Map[VertexId, Double],
        parent: Map[VertexId, VertexId]
    ): Map[VertexId, VertexId] =
      if (links.isEmpty) parent
      else {
        ...
        if (gain <= 0) parent
```

## 10. Generics and context bounds

`[VD: ClassTag]` means "for any vertex type `VD` for which a `ClassTag` exists". Spark needs the runtime class to
build arrays of `VD` on the executors, because the JVM erases generic types.

[`graph/WeightedPageRank.scala:47`](../../spark/src/main/scala/velograph/graph/WeightedPageRank.scala#L47)

```scala
  def transitionProbabilities[VD: ClassTag](graph: Graph[VD, Double]): Graph[Double, Double] = {
    val outStrength =
      graph.aggregateMessages[Double](ctx => ctx.sendToSrc(ctx.attr), _ + _, TripletFields.EdgeOnly)
    graph
      .outerJoinVertices(outStrength)((_, _, strength) => strength.getOrElse(0.0))
      .mapTriplets(t => t.attr / t.srcAttr, TripletFields.Src)
  }
```

**Upper bounds** work the same way: `def weighted[E <: Params](e: E): E` accepts any MLlib stage and returns the same
type it was given.

## 11. Implicits: extension methods

An **implicit class** adds a method to a type you do not own. `extends AnyVal` makes it a value class, so no wrapper
object is created at run time.

[`common/LakePaths.scala:23`](../../spark/src/main/scala/velograph/common/LakePaths.scala#L23)

```scala
  /** Spark expects URIs rather than OS paths; `file:///C:/...` keeps Windows drive letters intact. */
  implicit final class PathOps(private val path: Path) extends AnyVal {
    def uri: String = path.toUri.toString
  }
```

After `import velograph.common.LakePaths.PathOps`, any `java.nio.file.Path` has a `.uri`, as in
`recordings.uri` in the live backtest. `import spark.implicits._` uses the same mechanism to supply the encoders
behind `.as[StationStatus]` and `$"column"`.

## 12. `Option.fold`, default and named arguments, string interpolation

[`ml/FeaturePipeline.scala:80`](../../spark/src/main/scala/velograph/ml/FeaturePipeline.scala#L80): no weight column,
keep the stage; a weight column, set it.

```scala
    def weighted[E <: Params](e: E): E = weightCol.fold(e)(w =>
      e.copy(ParamMap(e.getParam("weightCol").asInstanceOf[Param[Any]] -> w)).asInstanceOf[E]
    )
```

Further examples:
- **Default arguments**: `rows: Option[Long] = None` in
  [`common/ServingJson.scala:30`](../../spark/src/main/scala/velograph/common/ServingJson.scala#L30).
- **Named arguments**: `TripLite(rideId = f(0), ...)` in `parseTrip` above.
- **String interpolation**: `s"velograph-$jobName"` and `s"bad chart option '$option' in '$spec'"`.

## 13. Lazy iterators

[`streaming/LiveRules.scala:234`](../../spark/src/main/scala/velograph/streaming/LiveRules.scala#L234): the
rebalancer takes each district's neediest station in turn.

```scala
    val needs = Iterator
      .iterate(districts)(_.map(_.tail).filter(_.nonEmpty))
      .takeWhile(_.nonEmpty)
      .flatMap(_.map(_.head))
      .toList
```

`Iterator.iterate` describes an endless sequence, and `takeWhile` stops it. Nothing runs until `toList` asks for the
values. Spark transformations are lazy in the same way: nothing runs until an action.

## 14. Where mutation is used, on purpose

Idiomatic Scala does not mean "never mutate". It means mutation stays local and has a reason:
- **The CSV splitter** (`rdd/CsvLine.scala`) uses a `while` loop, a `StringBuilder` and two `var`s. It runs on every
  one of the 4.98 million lines of the RDD lab, so it avoids creating short-lived objects.
- **The rebalancer** (`streaming/LiveRules.scala:240`) keeps the spare bikes per donor in a `mutable.Map` while it
  plans. The map is created and discarded inside one function call and never escapes, and the function still
  returns an immutable `Seq[Move]`.

## Likely viva questions

**`val`, `var` or `lazy val`?** `val` when the reference never changes, which is almost always here. `var` only for
local loop state. `lazy val` when the value is costly and may not be needed.

**Case class or plain class?** A case class for data: immutable fields, equality by value, pattern matching, Spark
encoders. A plain class when identity or hidden state matters.

**Why `Option` and not `null`?** The type says a value may be missing, and `map`, `filter` and `getOrElse` handle it
without a `NullPointerException`.

**Why `Try` in `SparkJob`?** The job report must be written whether the job succeeds or fails. `Try` turns the
exception into a value, so the report records it, and only then is it re-thrown.

**What does `@tailrec` guarantee?** That the recursion compiles to a loop. If the call is not in tail position, the
build fails instead of the job overflowing the stack later.

**What is a `ClassTag` for?** The JVM erases generic types. A `ClassTag` carries the runtime class, so Spark can
create typed arrays on the executors.

**Trait or abstract class?** A class can mix in several traits but extend only one class. `SparkJob` is a trait
because a job may want to extend something else.
