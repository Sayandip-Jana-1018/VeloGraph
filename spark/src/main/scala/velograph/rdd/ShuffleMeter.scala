package velograph.rdd

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerJobEnd,
  SparkListenerJobStart,
  SparkListenerTaskEnd
}

/** A measured block of work: its result, its wall-clock time and the bytes its jobs wrote to the shuffle. */
final case class Measured[A](value: A, millis: Long, shuffleBytes: Long)

/**
 * Measures shuffle bytes per labelled block of work from task metrics, the numbers the Spark UI shows as
 * "Shuffle Write". Jobs are attributed through their job group. Listener events arrive asynchronously, so
 * [[measure]] waits until every job it started has been seen to end before it reads the total.
 */
final class ShuffleMeter(sc: SparkContext) extends SparkListener {

  private final class Totals {
    val bytes                          = new AtomicLong
    val started                        = new AtomicInteger
    val ended                          = new AtomicInteger
    @volatile var lastEventNanos: Long = System.nanoTime()
  }

  private val byLabel    = new ConcurrentHashMap[String, Totals]()
  private val stageLabel = new ConcurrentHashMap[Int, String]()
  private val jobLabel   = new ConcurrentHashMap[Int, String]()

  sc.addSparkListener(this)

  private def touched(label: String): Totals = {
    val totals = byLabel.computeIfAbsent(label, _ => new Totals)
    totals.lastEventNanos = System.nanoTime()
    totals
  }

  override def onJobStart(event: SparkListenerJobStart): Unit =
    for (label <- Option(event.properties).flatMap(p => Option(p.getProperty("spark.jobGroup.id")))) {
      jobLabel.put(event.jobId, label)
      event.stageIds.foreach(stageLabel.put(_, label))
      touched(label).started.incrementAndGet()
    }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit =
    for (label <- Option(stageLabel.get(event.stageId)); metrics <- Option(event.taskMetrics))
      touched(label).bytes.addAndGet(metrics.shuffleWriteMetrics.bytesWritten)

  override def onJobEnd(event: SparkListenerJobEnd): Unit =
    for (label <- Option(jobLabel.remove(event.jobId))) touched(label).ended.incrementAndGet()

  /** Runs `body` (which must end in an action) under a fresh `label` and reports what it cost. */
  def measure[A](label: String)(body: => A): Measured[A] = {
    require(!byLabel.containsKey(label), s"label '$label' already measured")
    sc.setJobGroup(label, label)
    val start = System.nanoTime()
    val value = try body
    finally sc.clearJobGroup()
    val millis = (System.nanoTime() - start) / 1000000

    // The scheduler posts a job's end event before the action returns, but the listener may still be catching up:
    // wait until it has seen every job of this label end, and 200 ms without new events.
    val deadline         = System.nanoTime() + 10000000000L
    def settled: Boolean = Option(byLabel.get(label)).exists { t =>
      t.started.get > 0 && t.started.get == t.ended.get && System.nanoTime() - t.lastEventNanos > 200000000L
    }
    while (!settled && System.nanoTime() < deadline) Thread.sleep(25)
    Measured(value, millis, Option(byLabel.get(label)).map(_.bytes.get).getOrElse(0L))
  }
}
