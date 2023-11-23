import gears.async.{Async, Future, Supervised, AsyncSupport, uninterruptible, given}
import gears.async.Async.Source
import gears.async.AsyncOperations.*
import gears.async.default.given
import scala.util.boundary
import boundary.break
import scala.concurrent.duration.{Duration, DurationInt}
import java.util.concurrent.CancellationException
import scala.util.Success
import scala.util.Failure
import scala.util.Try

class SuperviseBehavior extends munit.FunSuite:
  test("each"):
    Async.blocking:
      val range = (0 to 9)
      val futs = range.map(i => Async.Source.values(i))
      assertEquals(Supervised.each(futs*), range)

  test("eachSuccess all succeed"):
    Async.blocking:
      assertEquals(Supervised.eachSuccess(), Seq.empty)
    Async.blocking:
      val range = (0 to 9)
      val futs = range.map(i => Async.Source.values(Success(i)))
      assertEquals(Supervised.eachSuccess(futs*), range)

  test("eachSuccess should fail immediately"):
    Async.blocking:
      val range = (0 to 9)
      val succFut = range.map(i => Async.Source.values(Success(i)))
      val exc = new Exception()
      val failFut = Async.Source.values(Failure(exc))
      val neverFut = Async.Source.values()
      val futs = succFut :+ failFut :+ neverFut
      assertEquals(Try(Supervised.eachSuccess(futs*)), Failure(exc))

  test("eachRight all right"):
    Async.blocking:
      assertEquals(Supervised.eachRight(), Right(Seq.empty))
    Async.blocking:
      val range = (0 to 9)
      val futs = range.map(i => Async.Source.values(Right(i)))
      assertEquals(Supervised.eachRight(futs*), Right(range))

  test("eachRight should fail immediately"):
    Async.blocking:
      val range = (0 to 9)
      val fail = Left("Failed")
      val failFut = Async.Source.values(fail)
      val neverFut = Async.Source.values()
      val futs = range.map(i => Async.Source.values(Right(i))) :+ failFut :+ neverFut
      assertEquals(Supervised.eachRight(futs*), fail)

  test("raceRight gets something"):
    Async.blocking:
      val okFut = Source.values(Right(1))
      val neverFut = Source.values()
      assertEquals(Supervised.raceRight(okFut, neverFut), Right(1))
    Async.blocking:
      val okFut = Source.values(Right(1))
      val failFut = Source.values(Left(1))
      assertEquals(Supervised.raceRight(okFut, failFut), Right(1))

  test("raceRight empty"):
    Async.blocking:
      assertEquals(Supervised.raceRight(), Left(Seq.empty))

  test("raceRight reports all failures"):
    Async.blocking:
      val range = 1 to 10
      val futs = range.map(v => Source.values(Left(v)))
      assertEquals(Supervised.raceRight(futs*), Left(range))

end SuperviseBehavior
