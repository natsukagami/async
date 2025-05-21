package gears.async

import language.experimental.captureChecking
import caps.cap

import scala.concurrent.ExecutionContext
import scala.concurrent.{Future as StdFuture, Promise as StdPromise}
import scala.util.Try

/** Converters from Gears types to Scala API types and back. */
object ScalaConverters:
  extension [T](fut: StdFuture[T]^)
    /** Converts a [[scala.concurrent.Future Scala Future]] into a gears [[Future]]. Requires an
      * [[scala.concurrent.ExecutionContext ExecutionContext]], as the job of completing the returned [[Future]] will be
      * done through this context. Since [[scala.concurrent.Future Scala Future]] cannot be cancelled, the returned
      * [[Future]] will *not* clean up the pending job when cancelled.
      */
    def asGears(using ExecutionContext): Future[T]^{cap.rd, fut} =
      Future.withResolver[T, caps.CapSet]: resolver =>
        val f: scala.util.Try[T] -> Unit =
          // SAFETY: we already track the Future[T] as capturing a hidden cap.rd
          caps.unsafe.unsafeAssumePure(result => resolver.complete(result))
        fut.andThen(f(_))

  extension [T](fut: Future[T]^{cap.rd})
    /** Converts a gears [[Future]] into a Scala [[scala.concurrent.Future Scala Future]]. Note that if `fut` is
      * cancelled, the returned [[scala.concurrent.Future Scala Future]] will also be completed with
      * `Failure(CancellationException)`.
      */
    def asScala: StdFuture[T]^{fut} =
      val p = StdPromise[T]()
      fut.onComplete(Listener((res, _) => p.complete(res)))
      p.future
