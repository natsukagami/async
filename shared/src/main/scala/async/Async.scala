package gears.async

import language.experimental.captureChecking
import caps.unbox

import gears.async.Listener.NumberedLock
import gears.async.Listener.withLock

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable
import scala.util.boundary
import scala.annotation.targetName
import scala.languageFeature.implicitConversions

/** The async context: provides the capability to asynchronously [[Async.await await]] for [[Async.Source Source]]s, and
  * defines a scope for structured concurrency through a [[CompletionGroup]].
  *
  * As both a context and a capability, the idiomatic way of using [[Async]] is to be implicitly passed around
  * functions, as an `using` parameter:
  * {{{
  * def function()(using Async): T = ???
  * }}}
  *
  * It is not recommended to store [[Async]] in a class field, since it complicates scoping rules.
  *
  * @param support
  *   An implementation of the underlying asynchronous operations (suspend and resume). See [[AsyncSupport]].
  * @param scheduler
  *   An implementation of a scheduler, for scheduling computation as they are spawned or resumed. See [[Scheduler]].
  *
  * @see
  *   [[Async$.blocking Async.blocking]] for a way to construct an [[Async]] instance.
  * @see
  *   [[Async$.group Async.group]] and [[Future$.apply Future.apply]] for [[Async]]-subscoping operations.
  */
trait Async(using val support: AsyncSupport, val scheduler: support.Scheduler):
  /** Waits for completion of source `src` and returns the result. Suspends the computation.
    *
    * @see
    *   [[Async.Source.awaitResult]] and [[Async$.await]] for extension methods calling [[Async!.await]] from the source
    *   itself.
    */
  def await[T, Cap^](using caps.Contains[Cap, this.type])(src: Async.Source[T, Cap]^): T

  /** Returns the cancellation group for this [[Async]] context. */
  def group: CompletionGroup

  /** Returns an [[Async]] context of the same kind as this one, with a new cancellation group. */
  def withGroup(group: CompletionGroup): Async^

object Async:
  inline def widen[Cap^](ac: Async^, ac2: Async^{ac})(using caps.Contains[Cap, ac.type]): Async^{Cap^} = ac2.asInstanceOf

  private class Blocking(val group: CompletionGroup)(using support: AsyncSupport, scheduler: support.Scheduler)
      extends Async(using support, scheduler), caps.Capability:
    private val lock = ReentrantLock()
    private val condVar = lock.newCondition()

    /** Wait for completion of async source `src` and return the result */
    override def await[T, Cap^](using caps.Contains[Cap, this.type])(src: Async.Source[T, Cap]^): T =
      src
        .poll()
        .getOrElse:
          var result: Option[T] = None
          src.onComplete:
            Listener.acceptingListener: (t, _) =>
              lock.lock()
              try
                result = Some(t)
                condVar.signalAll()
              finally lock.unlock()

          lock.lock()
          try
            while result.isEmpty do condVar.await()
            result.get
          finally lock.unlock()

    /** An Async of the same kind as this one, with a new cancellation group */
    override def withGroup(group: CompletionGroup): Async^ = Blocking(group)

  /** Execute asynchronous computation `body` on currently running thread. The thread will suspend when the computation
    * waits.
    */
  def blocking[T](body: Async.Spawn^ ?=> T)(using support: AsyncSupport, scheduler: support.Scheduler): T =
    group(using Blocking(CompletionGroup.Unlinked))(body)

  /** Returns the currently executing Async context. Equivalent to `summon[Async]`. */
  inline def current(using async: Async^): async.type = async

  /** [[Async.Spawn]] is a special subtype of [[Async]], also capable of spawning runnable [[Future]]s.
    *
    * Most functions should not take [[Spawn]] as a parameter, unless the function explicitly wants to spawn "dangling"
    * runnable [[Future]]s. Instead, functions should take [[Async]] and spawn scoped futures within [[Async.group]].
    */
  opaque type Spawn <: Async = Async

  /** Runs `body` inside a spawnable context where it is allowed to spawn concurrently runnable [[Future]]s. When the
    * body returns, all spawned futures are cancelled and waited for.
    */
  def group[T](using ac: Async^)(body: Async.Spawn^{ac} ?=> T): T =
    withNewCompletionGroup(CompletionGroup().link())(body)

  /** Runs a body within another completion group. When the body returns, the group is cancelled and its completion
    * awaited with the `Unlinked` group.
    */
  private[async] def withNewCompletionGroup[T](group: CompletionGroup)(using ac: Async^)(body: Async.Spawn^{ac} ?=> T): T =
    val completionAsync =
      if CompletionGroup.Unlinked == ac.group
      then ac
      else ac.withGroup(CompletionGroup.Unlinked)

    try body(using ac.withGroup(group))
    finally
      group.cancel()
      group.waitCompletion()(using completionAsync)

  /** An asynchronous data source. Sources can be persistent or ephemeral. A persistent source will always pass same
    * data to calls of [[Source!.poll]] and [[Source!.onComplete]]. An ephemeral source can pass new data in every call.
    *
    * @see
    *   An example of a persistent source is [[gears.async.Future]].
    * @see
    *   An example of an ephemeral source is [[gears.async.Channel]].
    */
  trait Source[+T, Cap^]:
    /** The unique symbol representing the current source. */
    val symbol: SourceSymbol[T] = SourceSymbol.next
    /** Checks whether data is available at present and pass it to `k` if so. Calls to `poll` are always synchronous and
      * non-blocking.
      *
      * The process is as follows:
      *   - If no data is immediately available, return `false` immediately.
      *   - If there is data available, attempt to lock `k`.
      *     - If `k` is no longer available, `true` is returned to signal this source's general availability.
      *     - If locking `k` succeeds:
      *       - If data is still available, complete `k` and return true.
      *       - Otherwise, unlock `k` and return false.
      *
      * Note that in all cases, a return value of `false` indicates that `k` should be put into `onComplete` to receive
      * data in a later point in time.
      *
      * @return
      *   Whether poll was able to pass data to `k`. Note that this is regardless of `k` being available to receive the
      *   data. In most cases, one should pass `k` into [[Source!.onComplete]] if `poll` returns `false`.
      */
    def poll(k: Listener[T]^{Cap^}): Boolean

    /** Once data is available, pass it to the listener `k`. `onComplete` is always non-blocking.
      *
      * Note that `k`'s methods will be executed on the same thread as the [[Source]], usually in sequence. It is hence
      * important that the listener itself does not perform expensive operations.
      */
    def onComplete(k: Listener[T]^{Cap^}): Unit

    /** Signal that listener `k` is dead (i.e. will always fail to acquire locks from now on), and should be removed
      * from `onComplete` queues.
      *
      * This permits original, (i.e. non-derived) sources like futures or channels to drop the listener from their
      * waiting sets.
      */
    def dropListener(k: Listener[T]^{Cap^}): Unit

    /** Similar to [[Async.Source!.poll(k:Listener[T])* poll]], but instead of passing in a listener, directly return
      * the value `T` if it is available.
      */
    def poll(): Option[T] =
      var resultOpt: Option[T] = None
      poll(Listener.acceptingListener { (x, _) => resultOpt = Some(x) })
      resultOpt

    /** Waits for an item to arrive from the source. Suspends until an item returns.
      *
      * This is an utility method for direct waiting with `Async`, instead of going through listeners.
      */
    final def awaitResult(using ac: Async^{Cap^}): T = ac.await[T, Cap](this)
  end Source

  // an opaque identity for symbols
  opaque type SourceSymbol[+T] = Long
  private [Async] object SourceSymbol:
    private val index = AtomicLong()
    inline def next: SourceSymbol[Any] =
      index.incrementAndGet()
  // ... it can be quickly obtained from any Source
  given[T, Cap^]: scala.Conversion[Source[T, Cap], SourceSymbol[T]] = _.symbol

  extension [T, Cap^](src: Source[scala.util.Try[T], Cap]^)
    /** Waits for an item to arrive from the source, then automatically unwraps it. Suspends until an item returns.
      * @see
      *   [[Source!.awaitResult awaitResult]] for non-unwrapping await.
      */
    @targetName("awaitTry")
    def await(using ac: Async^{Cap^}): T = src.awaitResult.get
  extension [E, T, Cap^](src: Source[Either[E, T], Cap]^)
    /** Waits for an item to arrive from the source, then automatically unwraps it. Suspends until an item returns.
      * @see
      *   [[Source!.awaitResult awaitResult]] for non-unwrapping await.
      */
    @targetName("awaitEither")
    def await(using ac: Async^{Cap^}) = src.awaitResult.right.get

  /** An original source has a standard definition of [[Source.onComplete onComplete]] in terms of [[Source.poll poll]]
    * and [[OriginalSource.addListener addListener]].
    *
    * Implementations should be the resource owner to handle listener queue and completion using an object monitor on
    * the instance.
    */
  abstract class OriginalSource[+T, Cap^] extends Source[T, Cap]:
    /** Add `k` to the listener set of this source. */
    protected def addListener(k: Listener[T]^{Cap^}): Unit

    def onComplete(k: Listener[T]^{Cap^}): Unit = synchronized:
      if !poll(k) then addListener(k)

  end OriginalSource

  object Source:
    /** Create a [[Source]] containing the given values, resolved once for each.
      *
      * @return
      *   an ephemeral source of values arriving to listeners in a queue. Once all values are received, attaching a
      *   listener with [[Source!.onComplete onComplete]] will be a no-op (i.e. the listener will never be called).
      */
    def values[T, Cap^](values: T*) =
      import scala.collection.JavaConverters._
      val q = java.util.concurrent.ConcurrentLinkedQueue[T]()
      q.addAll(values.asJavaCollection)
      new Source[T, Cap]:
        override def poll(k: Listener[T]^{Cap^}): Boolean =
          if q.isEmpty() then false
          else if !k.acquireLock() then true
          else
            val item = q.poll()
            if item == null then
              k.releaseLock()
              false
            else
              k.complete(item, this)
              true

        override def onComplete(k: Listener[T]^{Cap^}): Unit = poll(k)
        override def dropListener(k: Listener[T]^{Cap^}): Unit = ()
    end values

  extension [T, Cap^](src: Source[T, Cap]^)
    /** Create a new source that requires the original source to run the given transformation function on every value
      * received.
      *
      * Note that `f` is **always** run on the computation that produces the values from the original source, so this is
      * very likely to run **sequentially** and be a performance bottleneck.
      *
      * @param f
      *   the transformation function to be run on every value. `f` is run *before* the item is passed to the
      *   [[Listener]].
      */
    def transformValuesWith[U](f: (T -> U)^{Cap^}): Source[U, Cap]^{f, src} =
      new Source[U, Cap]:
        val selfSrc = this
        def transform(k: Listener[U]^{Cap^}): Listener.ForwardingListener[T]^{k, f} =
          new Listener.ForwardingListener[T](selfSrc, k):
            val lock = k.lock
            def complete(data: T, source: SourceSymbol[T]) =
              k.complete(f(data), selfSrc)

        def poll(k: Listener[U]^{Cap^}): Boolean =
          src.poll(transform(k))
        def onComplete(k: Listener[U]^{Cap^}): Unit =
          src.onComplete(transform(k))
        def dropListener(k: Listener[U]^{Cap^}): Unit =
          src.dropListener(transform(k))

  /** Creates a source that "races" a list of sources.
    *
    * Listeners attached to this source is resolved with the first item arriving from one of the sources. If multiple
    * sources are available at the same time, one of the items will be returned with no priority. Items that are not
    * returned are '''not''' consumed from the upstream sources.
    *
    * @see
    *   [[raceWithOrigin]] for a race source that also returns the upstream origin of the item.
    * @see
    *   [[Async$.select Async.select]] for a convenient syntax to race sources and awaiting them with [[Async]].
    */
  def race[T, Cap^](@caps.unbox sources: Seq[Source[T, Cap]^]): Source[T, Cap]^{sources*} = raceImpl((v: T, _: SourceSymbol[T]) => v)(sources)
  def race[T, Cap^](source: Source[T, Cap]^): Source[T, Cap]^{source} = race(Seq(source))
  def race[T, Cap^](s1: Source[T, Cap]^, s2: Source[T, Cap]^): Source[T, Cap]^{s1, s2} = race(Seq(s1, s2))

  /** Like [[race]], but the returned value includes a reference to the upstream source that the item came from.
    * @see
    *   [[Async$.select Async.select]] for a convenient syntax to race sources and awaiting them with [[Async]].
    */
  def raceWithOrigin[T, Cap^](@caps.unbox sources: Seq[Source[T, Cap]^]): Source[(T, SourceSymbol[T]), Cap]^{sources*} =
    raceImpl((v: T, src: SourceSymbol[T]) => (v, src))(sources)

  /** Pass first result from any of `sources` to the continuation */
  private def raceImpl[T, U, Cap^](map: (U, SourceSymbol[U]) -> T)(@caps.unbox sources: Seq[Source[U, Cap]^]): Source[T, Cap]^{sources*} =
    import caps.unsafe.unsafeAssumePure
    new Source[T, Cap]:
      val selfSrc = this
      def poll(k: Listener[T]^{Cap^}): Boolean =
        val it = sources.iterator
        var found = false

        val listener: Listener[U]^{k} = new Listener.ForwardingListener[U](selfSrc, k):
          val lock = k.lock
          def complete(data: U, source: SourceSymbol[U]) =
            k.complete(map(data, source), selfSrc)
        end listener

        while it.hasNext && !found do found = it.next.poll(listener)

        found

      def dropAll(l: Listener[U]^{sources*, Cap^}) =
        sources.foreach(_.dropListener(l.unsafeAssumePure))

      def onComplete(k: Listener[T]^{Cap^}): Unit =
        val listener: Listener[U]^{Cap^} = caps.unsafe.unsafeAssumePure(new Listener.ForwardingListener[U](this, k) {
          val self = this
          inline def lockIsOurs = k.lock == null
          val lock =
            if k.lock != null then
              // if the upstream listener holds a lock already, we can utilize it.
              new Listener.ListenerLock:
                val selfNumber = k.lock.selfNumber
                override def acquire() =
                  if found then false // already completed
                  else if !k.lock.acquire() then
                    if !found && !synchronized { // getAndSet alternative, avoid racing only with self here.
                        val old = found
                        found = true
                        old
                      }
                    then dropAll(self) // same as dropListener(k), but avoids an allocation
                    false
                  else if found then
                    k.lock.release()
                    false
                  else true
                override def release() = k.lock.release()
            else
              new Listener.ListenerLock with NumberedLock:
                val selfNumber: Long = number
                def acquire() =
                  if found then false
                  else
                    acquireLock()
                    if found then
                      releaseLock()
                      // no cleanup needed here, since we have done this by an earlier `complete` or `lockNext`
                      false
                    else true
                def release() =
                  releaseLock()

          var found = false

          def complete(item: U, src: SourceSymbol[U]) =
            found = true
            if lockIsOurs then lock.release()
            sources.foreach(s => if s.symbol != src then s.dropListener(caps.unsafe.unsafeAssumePure(self)))
            k.complete(map(item, src), selfSrc)
        }) // end listener

        sources.foreach(_.onComplete(listener))

      def dropListener(k: Listener[T]^{Cap^}): Unit =
        val listener = Listener.ForwardingListener.empty(this, k)
        sources.foreach(_.dropListener(listener.unsafeAssumePure))


  /** Cases for handling async sources in a [[select]]. [[SelectCase]] can be constructed by extension methods `handle`
    * of [[Source]].
    *
    * @see
    *   [[handle Source.handle]] (and its operator alias [[~~> ~~>]])
    * @see
    *   [[Async$.select Async.select]] where [[SelectCase]] is used.
    */
  trait SelectCase[+T, Cap^]:
    type Src
    val src: Source[Src, Cap]^{Cap^}
    val f: Src => T
    inline final def apply(input: Src) = f(input)

  extension [T, Cap^](_src: Source[T, Cap]^{Cap^})
    /** Attach a handler to `src`, creating a [[SelectCase]].
      * @see
      *   [[Async$.select Async.select]] where [[SelectCase]] is used.
      */
    def handle[U](_f: T => U): SelectCase[U, Cap]^{_src, _f} = new SelectCase:
      type Src = T
      val src = _src
      val f = _f

    /** Alias for [[handle]]
      * @see
      *   [[Async$.select Async.select]] where [[SelectCase]] is used.
      */
    /* TODO: inline after cc-ing channels */
    def ~~>[U](_f: T => U): SelectCase[U, Cap]^{_src, _f} = _src.handle(_f)

  /** Race a list of sources with the corresponding handler functions, once an item has come back. Like [[race]],
    * [[select]] guarantees exactly one of the sources are polled. Unlike [[transformValuesWith]], the handler in
    * [[select]] is run in the same async context as the calling context of [[select]].
    *
    * @see
    *   [[handle Source.handle]] (and its operator alias [[~~> ~~>]]) for methods to create [[SelectCase]]s.
    * @example
    *   {{{
    * // Race a channel read with a timeout
    * val ch = SyncChannel[Int]()
    * // ...
    * val timeout = Future(sleep(1500.millis))
    *
    * Async.select(
    *   ch.readSrc.handle: item =>
    *     Some(item * 2),
    *   timeout ~~> _ => None
    * )
    *   }}}
    */
  def select[T, Cap^](@caps.unbox cases: Seq[SelectCase[T, Cap]^{Cap^}])(using ac: Async^{Cap^}): T =
    val (input, which) = raceWithOrigin(cases.map(_.src)).awaitResult
    val sc = cases.find(_.src.symbol == which).get
    sc(input.asInstanceOf[sc.Src])
  def select[T, Cap^](c1: SelectCase[T, Cap^])(using ac: Async^{Cap^}): T = select(Seq(c1))
  def select[T, Cap^](c1: SelectCase[T, Cap^], c2: SelectCase[T, Cap^])(using ac: Async^{Cap^}): T = select(Seq(c1, c2))

  /** Race two sources, wrapping them respectively in [[Left]] and [[Right]] cases.
    * @return
    *   a new [[Source]] that resolves with [[Left]] if `src1` returns an item, [[Right]] if `src2` returns an item,
    *   whichever comes first.
    * @see
    *   [[race]] and [[select]] for racing more than two sources.
    */
  def either[T1, T2, Cap^](src1: Source[T1, Cap]^{Cap^}, src2: Source[T2, Cap]^{Cap^}): Source[Either[T1, T2], Cap]^{Cap^} =
    val left = src1.transformValuesWith(Left(_))
    val right = src2.transformValuesWith(Right(_))
    race(Seq(left, right))
end Async

