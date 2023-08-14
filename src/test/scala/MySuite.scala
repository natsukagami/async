import scala.concurrent.ExecutionContext
import scala.util.boundary
import scala.util.{Success, Failure}
// For more information on writing tests, see
// https://scalameta.org/munit/docs/getting-started.html
class MySuite extends munit.FunSuite {
  test("example test that succeeds") {
    val obtained = 42
    val expected = 42
    assertEquals(obtained, expected)
  }

  test("BoxChan") {
    import concurrent._

    val chan = BoxChan[Int]()
    val n = 1000

    given ExecutionContext = ExecutionContext.global

    Async.blocking:
      val f1 = Future:
          for i <- 1 to n do chan.send(i)
          println("f1 done")
      val f2 = Future:
          for i <- 1 to n do chan.send(-i)
          println("f2 done")
      val frecv = Future:
          for i <- 1 to (2 * n) do
            val r = chan.read()
            println(s"got $r (i = $i)")
          println("frecv done")

      frecv.value
  }

  test("BoxChan cancel") {
    import concurrent._

    val chan = BoxChan[Int]()
    val n = 1000

    given ExecutionContext = ExecutionContext.global

    Async.blocking:
      val f1 = Future:
          for i <- 1 to n do chan.send(i)
          chan.cancel()
      val f2 = Future:
          for i <- 1 to 10 * n do chan.send(-i)
          println("f2 done")
      val frecv = Future:
          boundary:
              for i <- 1 to (2 * n) do
                val r = chan.read()
                r match
                  case Success(v) =>
                    println(s"got $r (i = $i)")
                  case Failure(_) => boundary.break(())
              println("frecv done")

      frecv.value
  }
}
