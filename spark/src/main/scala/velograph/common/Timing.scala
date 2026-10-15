package velograph.common

object Timing {

  /** Evaluates `block` and returns its result together with the elapsed wall-clock milliseconds. */
  def timed[A](block: => A): (A, Long) = {
    val start  = System.nanoTime()
    val result = block
    (result, (System.nanoTime() - start) / 1000000L)
  }
}
