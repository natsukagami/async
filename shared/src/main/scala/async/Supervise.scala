package gears.async

import scala.collection.Set
import scala.collection.mutable
import gears.async.Async.Source
import scala.util.{Try, Success, Failure}

object Supervised:
  /** Return the first [[Right]] value, or the list of all errors if all fails. */
  def raceRight[E, T](sources: Source[Either[E, T]]*)(using Async) =
    val errs = mutable.Map[Source[Either[E, T]], E]()
    withHandler(sources*): (item, origin, sourceSet) =>
      item match
        case Right(value) => Left(Right(value))
        case Left(err) =>
          errs += (origin -> err)
          if sourceSet.size == 1 then Left(Left(sources.map(errs(_))))
          else Right(sourceSet - origin)
    .getOrElse(Left(Seq.empty)) // source set is empty case

  /** Collect values from [[sources]] while [[f]] returns [[Some]] on the item.
    * Returns when all sources stops being collected.
    */
  def takeWhile[T, U](sources: Source[T]*)(f: T => Option[U])(using Async) =
    val buf = mutable.ArrayBuffer[U]()
    withHandler(sources*): (item, origin, sources) =>
      f(item) match
        case None => Right(sources - origin)
        case Some(value) => buf += value; Right(sources)
    buf.toSeq

  /** Await for one value from each source, immediately returning the error on [[Left]].
    * When error happens, all received values are dropped.
    */
  def eachRight[E, T](sources: Source[Either[E, T]]*)(using Async) =
    val map = mutable.Map[Source[Either[E, T]], T]()
    withHandler(sources*): (item, origin, srcs) =>
      item match
        case Left(exc) => Left(Left(exc))
        case Right(v) =>
          map += (origin -> v)
          if srcs.size == 1 then
            Left(Right(sources.map(map(_))))
          else
            Right(srcs - origin)
    .getOrElse(Right(Seq.empty) /* Empty sources case */)

  /** Await for one value from each source, immediately re-throwing the exception on [[Failure]]. */
  def eachSuccess[T](sources: Source[Try[T]]*)(using Async) =
    val map = mutable.Map[Source[Try[T]], T]()
    withHandler(sources*): (item, origin, sources) =>
      item match
        case Success(v) => map += (origin -> v)
        case Failure(exc) => throw exc
      if sources.size == 1 then Left(()) else Right(sources - origin)
    sources.map(map(_))

  /** Await for one value from each source. */
  def each[T](sources: Source[T]*)(using Async) =
    val map = mutable.Map[Source[T], T]()
    withHandler(sources*): (item, origin, sources) =>
      map += (origin -> item)
      if sources.size == 1 then Left(()) else Right(sources - origin)
    sources.map(map(_))

  /** A generic Handler for supervision.
    * Takes a received [[T]] alongside with its [[Source]],
    * and the list of currently subscribed sources.
    * It can either:
    * - Update the list of subscribed sources (e.g. removing the received origin, spawn more sources, ...), or
    * - Immediately return a final value of type [[U]].
    */
  type Handler[T, U] = (T, Source[T], Set[Source[T]]) => Async ?=> Either[U, Set[Source[T]]]

  /** Supervise the given sources with a [[handler]], exiting when the handler returns
    * a value (wrapped in [[Option.Some]] or an empty set (returning [[Option.None]]).
    */
  def withHandler[T, U](sources: Source[T]*)(handler: Handler[T, U])(using Async): Option[U] =
      var srcs = Set(sources*)
      while !srcs.isEmpty do
        val (item, origin) = Async.race(
           srcs.toSeq.map(src => src.map((_, src)))*
         ).await
        handler(item, origin, srcs) match
          case Left(value) => return Some(value)
          case Right(value) => srcs = value
      None
