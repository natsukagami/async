import scala.concurrent.ExecutionContext
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
    val n = 100

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
}
