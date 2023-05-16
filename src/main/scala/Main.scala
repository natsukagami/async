import concurrent.*
import scala.concurrent.ExecutionContext
import scala.annotation.tailrec

@main def hello: Unit =
  val n = 2000
  var t = 0
  for (i <- 1 to n)
    ExecutionContext.global.execute: () =>
      Thread.sleep(10)
      println(s"Hello world! $i")
      synchronized:
        t += 1
        notify()
  waitUntil(t == n)

  @tailrec def waitUntil(f: => Boolean): Unit =
    val nx = synchronized:
      if f then true
      else
        wait()
        false
    if !nx then waitUntil(f)


