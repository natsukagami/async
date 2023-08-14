package concurrent

import scala.collection.mutable
import concurrent.Async._
import concurrent.Future._
import scala.util.{Try, Success, Failure}
import scala.concurrent.Channel
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.CancellationException

type TryListener[T] = Listener[Try[T]]

/** Queue is a less complete form of rendevous channel, where sends (or `store`)
  * are not blocking, but also not asynchronous.
  */
class Queue[T] extends Async.OriginalSource[Try[T]], Cancellable:
  private val listeners = mutable.Set[TryListener[T]]()
  private var cancelled = false

  protected def addListener(k: TryListener[T]): Unit = synchronized:
      if !cancelled then listeners += k
      else k(Failure(CancellationException()))

  def dropListener(k: TryListener[T]): Unit = synchronized {
    listeners -= k
  }

  override def poll(k: TryListener[T]): Boolean = false

  def cancel() = synchronized:
      cancelled = true

  /** Tries to enqueue the item, return `false` if the item is not immediately
    * enqueue-able.
    */
  def send(item: Try[T]): Boolean = synchronized:
      if cancelled then false
      else
        listeners.find(_(item)) match
          case None => false
          case Some(v) =>
            listeners -= v
            true
  def send(item: T): Boolean = send(Success(item))

  /** Sends the item to all pending listeners. */
  def sendAll(item: Try[T]): Unit =
    synchronized:
        listeners.filterInPlace(_(item))
  def sendAll(item: T): Unit = sendAll(Success(item))
end Queue

/** Holds a single value of `T`.
  *
  *   - `put` blocks (asynchronously) until the item has been successfully
  *     deposited. TODO: * cancelling support?
  *   - `take` does not block, but is asynchronous: it will atomically take the
  *     `Box[T]` value if the given `listener` returns true. Otherwise, or if
  *     the box is empty, returns false *immediately*.
  *
  * Note that while the `Box` can be cancelled, the already stored item will
  * still be stored and consumed (once).
  */
class Box[T] extends Cancellable:
  private var inner: Option[T] = None
  private val queue = Queue[Unit]()

  def put(item: T)(using Async): Unit =
    val fut = synchronized:
        if inner.isEmpty then
          inner = Some(item)
          None
        else
          val p = Promise[Unit]()
          queue.onComplete: r =>
            Box.this.synchronized:
              inner = Some(item)
              p.complete(r)
              true
          Some(p.future)
    fut.foreach(await(_))

  override def cancel(): Unit = synchronized:
      queue.cancel()

  def take: Option[T] =
    var item: Option[T] = None
    take(v => { item = Some(v); true })
    item

  def take(k: Listener[T]): Boolean =
    synchronized:
        inner match
          case None => false
          case Some(item) =>
            if k(item) then
              inner = None
              queue.send(())
              true
            else false

class BoxChan[T] extends Cancellable:
  private val box = Box[(T, Promise[Unit])]()
  private val readers = Queue[T]()

  private val closed = AtomicBoolean(false)

  val canRead: Async.Source[Try[T]] = new Async.Source[Try[T]]:
    def poll(k: Listener[Try[T]]): Boolean = false
    def onComplete(k: Listener[Try[T]]): Unit =
      if closed.get() then k(Failure(CancellationException()))
      else if !sendItem(k) then
        readers.onComplete(k)
        sendItem(readers.send(_))
    def dropListener(k: Listener[Try[T]]): Unit = readers.dropListener(k)

  def read()(using Async): Try[T] = await(canRead)

  def send(item: T)(using Async): Unit =
    if closed.get() then throw CancellationException()
    val sent = Promise[Unit]()
    box.put((item, sent))
    sendItem(readers.send(_))
    sent.future.value

  def close(): Unit =
    if closed.compareAndSet(false, true) then
      box.cancel()
      box.take { (_, sent) =>
        sent.complete(Failure(CancellationException())); true
      }
      readers.cancel()

  def cancel(): Unit = close()

  private def sendItem(k: Listener[Try[T]]): Boolean =
    box.take { (item, sent) =>
      if k(Success(item)) then
        sent.complete(Success(()))
        true
      else false
    }
