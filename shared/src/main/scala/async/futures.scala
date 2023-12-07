package gears.async

import TaskSchedule.ExponentialBackoff
import AsyncOperations.sleep

import scala.collection.mutable
import mutable.ListBuffer

import scala.compiletime.uninitialized
import scala.util.{Failure, Success, Try}
import scala.annotation.unchecked.uncheckedVariance
import java.util.concurrent.CancellationException
import scala.annotation.tailrec
import scala.util
import java.util.concurrent.atomic.AtomicLong
import gears.async.Async.race

/** A cancellable future that can suspend waiting for other asynchronous sources
  */
trait Future[+T] extends Async.OriginalSource[T], Cancellable

object Future:

  /** A future that is completed explicitly by calling its `complete` method. There are two public implementations
    *
    *   - RunnableFuture: Completion is done by running a block of code
    *   - Promise.future: Completion is done by external request.
    */
  private class CoreFuture[+T] extends Future[T]:

    @volatile protected var hasCompleted: Boolean = false
    protected var cancelRequest = false
    private var result: Try[T] = uninitialized // guaranteed to be set if hasCompleted = true
    private val waiting: mutable.Set[Listener[T]] = mutable.Set()

    // Async.Source method implementations

    def poll(k: Listener[T]): Boolean =
      if hasCompleted then
        k.completeNow(result, this)
        true
      else false

    def addListener(k: Listener[T]): Unit = synchronized:
      if hasCompleted then k.completeNow(result, this)
      else waiting += k

    def dropListener(k: Listener[T]): Unit = synchronized:
      waiting -= k

    // Cancellable method implementations

    def cancel(): Unit =
      cancelRequest = true

    // Future method implementations

    override def awaitTry(using async: Async): Try[T] =
      val r = async.awaitTry(this)
      if cancelRequest then Failure(new CancellationException()) else r

    /** Complete future with result. If future was cancelled in the meantime, return a CancellationException failure
      * instead. Note: @uncheckedVariance is safe here since `complete` is called from only two places:
      *   - from the initializer of RunnableFuture, where we are sure that `T` is exactly the type with which the future
      *     was created, and
      *   - from Promise.complete, where we are sure the type `T` is exactly the type with which the future was created
      *     since `Promise` is invariant.
      */
    private[Future] def complete(result: Try[T] @uncheckedVariance): Unit =
      val toNotify = synchronized:
        if hasCompleted then Nil
        else
          this.result = result
          hasCompleted = true
          val ws = waiting.toList
          waiting.clear()
          ws
      for listener <- toNotify do listener.completeNow(result, this)

  end CoreFuture

  /** A future that is completed by evaluating `body` as a separate asynchronous operation in the given `scheduler`
    */
  private class RunnableFuture[+T](body: Async ?=> T)(using ac: Async) extends CoreFuture[T]:

    private var innerGroup: CompletionGroup = CompletionGroup()

    private def checkCancellation(): Unit =
      if cancelRequest then throw new CancellationException()

    private class FutureAsync(val group: CompletionGroup)(using
        label: ac.support.Label[Unit]
    ) extends Async(using ac.support, ac.scheduler):

      /** Await a source first by polling it, and, if that fails, by suspending in a onComplete call.
        */
      override def awaitTry[U](src: Async.Source[U]): Try[U] =
        class CancelSuspension extends Cancellable:
          var suspension: ac.support.Suspension[Try[U], Unit] = uninitialized
          var listener: Listener[U] = uninitialized
          var completed = false

          def complete() = synchronized:
            val completedBefore = completed
            completed = true
            completedBefore

          override def cancel() =
            val completedBefore = complete()
            if !completedBefore then
              src.dropListener(listener)
              ac.support.resumeAsync(suspension)(Failure(new CancellationException()))

        if group.isCancelled then throw new CancellationException()

        src
          .poll()
          .getOrElse:
            val cancellable = CancelSuspension()
            val res = ac.support.suspend[Try[U], Unit](k =>
              val listener = Listener.acceptingListener[U]: (x, _) =>
                val completedBefore = cancellable.complete()
                if !completedBefore then ac.support.resumeAsync(k)(x)
              cancellable.suspension = k
              cancellable.listener = listener
              cancellable.link(group) // may resume + remove listener immediately
              src.onComplete(listener)
            )
            cancellable.unlink()
            res

      override def withGroup(group: CompletionGroup) = FutureAsync(group)

    override def cancel(): Unit =
      super.cancel()
      this.innerGroup.cancel()

    link()
    ac.support.scheduleBoundary:
      Async.withNewCompletionGroup(innerGroup)(complete(Try({
        val r = body
        checkCancellation()
        r
      }).recoverWith { case _: InterruptedException | _: CancellationException =>
        Failure(new CancellationException())
      }))(using FutureAsync(CompletionGroup.Unlinked))
      signalCompletion()(using ac)

  end RunnableFuture

  /** Create a future that asynchronously executes `body` that defines its result value in a Try or returns failure if
    * an exception was thrown. If the future is created in an Async context, it is added to the children of that
    * context's root.
    */
  def apply[T](body: Async ?=> T)(using Async): Future[T] =
    RunnableFuture(body)

  /** A future that immediately terminates with the given result */
  def now[T](result: Try[T]): Future[T] =
    val f = CoreFuture[T]()
    f.complete(result)
    f

  private inline def failPromiseOr[T, U](promise: Promise[T])(l: Async.Source[U])(body: U => Unit) =
    l.onComplete(Listener {
      case (Failure(exc), _) => promise.complete(Failure(exc))
      case (Success(u), _)   => body(u)
    })

  extension [T](f1: Future[T])

    /** Parallel composition of two futures. If both futures succeed, succeed with their values in a pair. Otherwise,
      * fail with the failure that was returned first.
      */
    def zip[U](f2: Future[U])(using Async): Future[(T, U)] =
      val promise = Promise[(T, U)]()
      failPromiseOr(promise)(Async.either(f1, f2)) {
        case Left(v)  => failPromiseOr(promise)(f2)(u => promise.complete(Success((v, u))))
        case Right(u) => failPromiseOr(promise)(f1)(v => promise.complete(Success((v, u))))
      }
      promise.future

    /** Parallel composition of tuples of futures. Future.Success(EmptyTuple) might be treated as Nil.
      */
    def *:[U <: Tuple](f2: Future[U])(using Async): Future[T *: U] =
      val promise = Promise[(T *: U)]()
      failPromiseOr(promise)(Async.either(f1, f2)) {
        case Left(v)  => failPromiseOr(promise)(f2)(u => promise.complete(Success((v *: u))))
        case Right(u) => failPromiseOr(promise)(f1)(v => promise.complete(Success((v *: u))))
      }
      promise.future

    /** Alternative parallel composition of this task with `other` task. If either task succeeds, succeed with the
      * success that was returned first. Otherwise, fail with the failure that was returned last.
      */
    def alt(f2: Future[T])(using Async): Future[T] = altImpl(false)(f2)

    /** Like `alt` but the slower future is cancelled. If either task succeeds, succeed with the success that was
      * returned first and the other is cancelled. Otherwise, fail with the failure that was returned last.
      */
    def altWithCancel(f2: Future[T])(using Async): Future[T] = altImpl(true)(f2)

    private inline def altImpl(inline withCancel: Boolean)(f2: Future[T])(using Async): Future[T] =
      val promise = Promise[T]()
      Async
        .raceWithOrigin(f1, f2)
        .onComplete(Listener { (v, _) =>
          v.get match
            case (f @ Success(v), src) =>
              inline if withCancel then (if src == f1 then f2 else f1).cancel()
              promise.complete(f)
            case (_, src) =>
              val remain = if src == f1 then f2 else f1
              failPromiseOr(promise)(remain)(v => promise.complete(Success(v)))
        })
      promise.future

  end extension

  /** A promise defines a future that is be completed via the promise's `complete` method.
    */
  class Promise[T]:
    private val myFuture = CoreFuture[T]()

    /** The future defined by this promise */
    val future: Future[T] = myFuture

    /** Define the result value of `future`. However, if `future` was cancelled in the meantime complete with a
      * `CancellationException` failure instead.
      */
    def complete(result: Try[T]): Unit = myFuture.complete(result)

  end Promise

  /** Collects a list of futures into a channel of futures, arriving as they finish. */
  class Collector[T](futures: Future[T]*):
    private val ch = UnboundedChannel[Future[T]]()

    /** Output channels of all finished futures. */
    final def results = ch.asReadable

    private val listener = Listener((_, fut) =>
      // safe, as we only attach this listener to Future[T]
      ch.sendImmediately(fut.asInstanceOf[Future[T]])
    )

    protected final def addFuture(future: Future[T]) = future.onComplete(listener)

    futures.foreach(addFuture)
  end Collector

  /** Like [[Collector]], but exposes the ability to add futures after creation. */
  class MutableCollector[T](futures: Future[T]*) extends Collector[T](futures*):
    /** Add a new [[Future]] into the collector. */
    def add(future: Future[T]) = addFuture(future)
    inline def +=(future: Future[T]) = add(future)

  extension [T](fs: Seq[Future[T]])
    /** `.await` for all futures in the sequence, returns the results in a sequence, or throws if any futures fail. */
    def awaitAll(using Async) =
      val collector = Collector(fs*)
      for _ <- fs do collector.results.read().get.await
      fs.map(_.await)

    /** Like [[awaitAll]], but cancels all futures as soon as one of them fails. */
    def awaitAllOrCancel(using Async) =
      val collector = Collector(fs*)
      try
        for _ <- fs do collector.results.read().get.await
        fs.map(_.await)
      catch
        case e: Exception =>
          fs.foreach(_.cancel())
          throw e

    /** Race all futures, returning the first successful value. Throws the last exception received, if everything fails.
      */
    def altAll(using Async): T = altImpl(false)

    /** Like [[altAll]], but cancels all other futures as soon as the first future succeeds. */
    def altAllWithCancel(using Async): T = altImpl(true)

    private inline def altImpl(withCancel: Boolean)(using Async): T =
      val collector = Collector(fs*)
      @scala.annotation.tailrec
      def loop(attempt: Int): T =
        collector.results.read().get.awaitTry match
          case Failure(exception) =>
            if attempt == fs.length then /* everything failed */ throw exception else loop(attempt + 1)
          case Success(value) =>
            inline if withCancel then fs.foreach(_.cancel())
            value
      loop(1)
end Future

/** TaskSchedule describes the way in which a task should be repeated. Tasks can be set to run for example every 100
  * milliseconds or repeated as long as they fail. `maxRepetitions` describes the maximum amount of repetitions allowed,
  * after that regardless of TaskSchedule chosen, the task is not repeated anymore and the last returned value is
  * returned. `maxRepetitions` equal to zero means that repetitions can go on potentially forever.
  */
enum TaskSchedule:
  case Every(val millis: Long, val maxRepetitions: Long = 0)
  case ExponentialBackoff(
      val millis: Long,
      val exponentialBase: Int = 2,
      val maxRepetitions: Long = 0
  )
  case FibonacciBackoff(val millis: Long, val maxRepetitions: Long = 0)
  case RepeatUntilFailure(val millis: Long = 0, val maxRepetitions: Long = 0)
  case RepeatUntilSuccess(val millis: Long = 0, val maxRepetitions: Long = 0)

/** A task is a template that can be turned into a runnable future Composing tasks can be referentially transparent.
  * Tasks can be also ran on a specified schedule.
  */
class Task[+T](val body: (Async, AsyncOperations) ?=> T):

  /** Start a future computed from the `body` of this task */
  def run(using Async, AsyncOperations) = Future(body)

  def schedule(
      s: TaskSchedule
  ): Task[T] =
    s match {
      case TaskSchedule.Every(millis, maxRepetitions) =>
        assert(millis >= 1)
        assert(maxRepetitions >= 0)
        Task {
          var repetitions = 0
          var ret: T = body
          repetitions += 1
          if (maxRepetitions == 1) ret
          else {
            while (maxRepetitions == 0 || repetitions < maxRepetitions) {
              sleep(millis)
              ret = body
              repetitions += 1
            }
            ret
          }
        }
      case TaskSchedule.ExponentialBackoff(
            millis,
            exponentialBase,
            maxRepetitions
          ) =>
        assert(millis >= 1)
        assert(exponentialBase >= 2)
        assert(maxRepetitions >= 0)
        Task {
          var repetitions = 0
          var ret: T = body
          repetitions += 1
          if (maxRepetitions == 1) ret
          else {
            var timeToSleep = millis
            while (maxRepetitions == 0 || repetitions < maxRepetitions) {
              sleep(timeToSleep)
              timeToSleep *= exponentialBase
              ret = body
              repetitions += 1
            }
            ret
          }
        }
      case TaskSchedule.FibonacciBackoff(millis, maxRepetitions) =>
        assert(millis >= 1)
        assert(maxRepetitions >= 0)
        Task {
          var repetitions = 0
          var a: Long = 0
          var b: Long = 1
          var ret: T = body
          repetitions += 1
          if (maxRepetitions == 1) ret
          else {
            sleep(millis)
            ret = body
            repetitions += 1
            if (maxRepetitions == 2) ret
            else {
              while (maxRepetitions == 0 || repetitions < maxRepetitions) {
                val aOld = a
                a = b
                b = aOld + b
                sleep(b * millis)
                ret = body
                repetitions += 1
              }
              ret
            }
          }
        }
      case TaskSchedule.RepeatUntilFailure(millis, maxRepetitions) =>
        assert(millis >= 0)
        assert(maxRepetitions >= 0)
        Task {
          @tailrec
          def helper(repetitions: Long = 0): T =
            if (repetitions > 0 && millis > 0)
              sleep(millis)
            val ret: T = body
            ret match {
              case Failure(_) => ret
              case _ if (repetitions + 1) == maxRepetitions && maxRepetitions != 0 =>
                ret
              case _ => helper(repetitions + 2)
            }
          helper()
        }
      case TaskSchedule.RepeatUntilSuccess(millis, maxRepetitions) =>
        assert(millis >= 0)
        assert(maxRepetitions >= 0)
        Task {
          @tailrec
          def helper(repetitions: Long = 0): T =
            if (repetitions > 0 && millis > 0)
              sleep(millis)
            val ret: T = body
            ret match {
              case Success(_) => ret
              case _ if (repetitions + 1) == maxRepetitions && maxRepetitions != 0 =>
                ret
              case _ => helper(repetitions + 2)
            }
          helper()
        }
    }

end Task

def uninterruptible[T](body: Async ?=> T)(using ac: Async): T =
  val tracker = Cancellable.Tracking().link()

  val r =
    try
      val group = CompletionGroup()
      Async.withNewCompletionGroup(group)(body)
    finally tracker.unlink()

  if tracker.isCancelled then throw new CancellationException()
  r

def cancellationScope[T](cancel: Cancellable)(fn: => T)(using a: Async): T =
  cancel.link()
  try fn
  finally cancel.unlink()
