import gears.async._
import gears.async.AsyncOperations._
import scala.concurrent.duration._
import scala.util.{Success, Failure}
import java.util.concurrent.TimeoutException
import java.util.concurrent.CancellationException

class TimerTest extends munit.FunSuite {
  import gears.async.default.given

  test("TimerSleep1Second") {
    Async.blocking:
      println("start of 1 second")
      val timer = Timer(1.second)
      Future { timer.start() }
      assertEquals(timer.src.await, timer.TimerEvent.Tick)
      println("end of 1 second")
  }

  def timeoutCancellableFuture[T](d: Duration, f: Future[T])(using Async, AsyncOperations): Future[T] =
    val t = Future { sleep(d.toMillis) }
    Future:
      val g = Async.either(t, f).await
      g match
        case Left(_) =>
          f.cancel()
          throw TimeoutException()
        case Right(v) =>
          t.cancel()
          v

  test("testTimeoutFuture") {
    var touched = false
    Async.blocking:
      val t = timeoutCancellableFuture(
        250.millis,
        Future:
          sleep(1000)
          touched = true
      )
      t.awaitTry
      assert(!touched)
      sleep(2000)
      assert(!touched)
  }
}
