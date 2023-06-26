package concurrent

import scala.collection.mutable
import concurrent.Async._
import concurrent.Future._
import scala.util.Success

/** Queue is a less complete form of rendevous channel, where sends (or `store`)
  * are not blocking, but also not asynchronous.
  */
class Queue[T] extends Async.OriginalSource[T]:
  private val listeners = mutable.Set[Listener[T]]()

  protected def addListener(k: Listener[T]): Unit = synchronized {
    listeners += k
  }

  def dropListener(k: Listener[T]): Unit = synchronized {
    listeners -= k
  }

  override def poll(k: Listener[T]): Boolean = false

  /** Tries to enqueue the item, return `false` if the item is not immediately
    * enqueue-able.
    */
  def send(item: T): Boolean =
    synchronized:
        listeners.find(_(item)) match
          case None => false
          case Some(v) =>
            listeners -= v
            true

  /** Sends the item to all pending listeners. */
  def sendAll(item: T): Unit =
    synchronized:
        listeners.filterInPlace(_(item))
end Queue

class Box[T]:
  private var inner: Option[T] = None
  private val queue = Queue[Unit]()

  def put(item: T)(using Async): Unit =
    val fut = synchronized:
        if inner.isEmpty then
          inner = Some(item)
          None
        else
          val p = Promise[Unit]()
          queue.onComplete { _ =>
            Box.this.synchronized:
              inner = Some(item)
            p.complete(Success(()))
            true
          }
          Some(p.future)
    fut.foreach(_.value)

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

class BoxChan[T]:
  private val box = Box[(T, Promise[Unit])]()
  private val readers = Queue[T]()

  val canRead: Async.Source[T] = new Async.Source[T]:
    def poll(k: Listener[T]): Boolean = false
    def onComplete(k: Listener[T]): Unit =
      if !sendItem(k) then
        readers.onComplete(k)
        sendItem(readers.send(_))
    def dropListener(k: Listener[T]): Unit = readers.dropListener(k)

  def read()(using Async): T = await(canRead)

  def send(item: T)(using Async): Unit =
    val sent = Promise[Unit]()
    box.put((item, sent))
    sendItem(readers.send(_))
    sent.future.value

  private def sendItem(k: Listener[T]): Boolean =
    box.take { (item, sent) =>
      if k(item) then
        sent.complete(Success(()))
        true
      else false
    }
