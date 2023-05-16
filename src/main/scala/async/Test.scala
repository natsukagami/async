// scalajs: --skip

import concurrent.*
import fiberRuntime.boundary.setName
import scala.concurrent.ExecutionContext

@main def Test =
  given ExecutionContext = ExecutionContext.global
  Async.blocking:
    val x = Future:
      setName("x")
      val a = Future{ setName("xa"); 22 }
      val b = Future{ setName("xb"); 11 }
      val c = Future { setName("xc"); assert(false); 1 }
      c.alt(Future{ setName("alt1"); a.value + b.value }).alt(c).value
    val y = Future:
      setName("y")
      val a = Future{ setName("ya"); 22 }
      val b = Future{ setName("yb"); 11 }
      a.zip(b).value
    val z = Future:
      val a = Future{ setName("za"); 22 }
      val b = Future{ setName("zb"); true }
      a.alt(b).value
    // val _: Future[Int | Boolean] = z
    val sum = Future:
      val nums = (1 to 1000).map(i => Future { i.toLong }).map(i => Future { i.value * i.value })
      nums.map(_.value).sum
    println("test async:")
    println(x.value)
    println(y.value)
    println(sum.value)
    // (1 to 100).map(_ => loopy()).foreach { x => println(x.value) }
  //println("test choices:")
  //println(TestChoices)


  def loopy()(using Async) =
    Future:
      val one = Future { 1 }
      def loop(x: Int): Int =
        if x == 0 then one.value else loop(x-1) + 1
      loop(10000)

