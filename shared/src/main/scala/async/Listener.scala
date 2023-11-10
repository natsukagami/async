package gears.async

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import scala.util.Try
import scala.util.boundary, boundary.break
import java.util.concurrent.Semaphore

object Listener:

  /** Lock two Listeners preventing deadlocks. If locking of both succeeds, both locks
   *  are returned as a tuple wrapped in a right. If one (the first) listener fails
   *  (unable to lock), then this listener instance is returned in a Left.
   */
  def lockBoth[T, V](l1: Listener[T], l2: Listener[V]): Listener[?] | (l1.Key, l2.Key) =
    def loop[T, V](l1: Listener[T], l2: Listener[V])(lk1: Locker[l1.Key] | Option[l1.Key], lk2: Locker[l2.Key] | Option[l2.Key]): Listener[?] | (l1.Key, l2.Key) =
      (lk1, lk2) match
        case (None, _) => l1
        case (_, None) => l2
        case (Some(k1), Some(k2)) => (k1, k2)
        case (lk1: Locker[l1.Key], lk2: Locker[l2.Key]) if lk1.number < lk2.number =>
          loop(l2, l1)(lk2, lk1) match
            case l: Listener[?] => l
            case (k1, k2) => (k2, k1)
        case (lk1: Locker[l1.Key], v2) =>
          val v1 = lk1.lock()
          val res = loop(l1, l2)(v1, v2)
          if res.isInstanceOf[Listener[?]] && v1 != None then lk1.release() // If locking fails, the locked layer must be released
          res
        case (v1, lk2: Locker[l2.Key]) =>
          val v2 = lk2.lock()
          val res = loop(l1, l2)(v1, v2)
          if res.isInstanceOf[Listener[?]] && v2 != None then lk2.release() // If locking fails, the locked layer must be released
          res
    loop(l1, l2)(l1.tryLock(), l2.tryLock())

  /** Create a listener that will always accept the element and pass it to the
   *  given consumer.
  */
  def acceptingListener[T](consumer: T => Unit): Listener[T] =
    new Listener[T]:
      type Key = Unit
      override def tryLock() = Some(())
      override def complete(item: T)(using Key) = consumer(item)
      override def completeNow(item: T): Boolean = { consumer(item); true }

  /** A listener for values that are processed by the given source `src` and
   *  that are demanded by the continuation listener `continue`.
   *  This class is necessary to identify listeners registered to upstream sources (for removal).
   */
  abstract case class ForwardingListener[T](src: Async.Source[?], continue: Listener[?]) extends Listener[T]

  def emptyForwarding[T](src: Async.Source[?], continue: Listener[?]): Listener[T] =
    new ForwardingListener[T](src, continue) {
      type Key = Nothing
      override def tryLock(): Option[Key] | Locker[Nothing] = ???
      override def complete(item: T)(using Key): Unit = ???
    }

  private val listenerNumber = AtomicLong()
  /** A listener that wraps an internal lock and that receives a number atomically
   *  for deadlock prevention. Note that numbered ForwardingListeners always have
   *  greater numbers than their wrapped `continue`-Listener.
   */
  class NumberedLock:
    val number = listenerNumber.getAndIncrement()
    private val lock0 = Semaphore(1)

    /** Locks this listener's internal lock *if the LockContext permits it*.
     *  May throw because of LockContext.
     */
    def lock() =
      lock0.acquire()

    def unlock() =
      lock0.release()

/** An atomic listener that may manage an internal lock to guarantee atomic completion
 */
trait Listener[-T]:
  /** Key represents the capability to send an item to the listener. Note that Key can be used only once. */
  type Key
  /** Try to obtain the key to send an item.
    * Returns None if the listener is no longer available.
    * If the listener utilizes a lock, returns the (yet locked) Locker instance.
    */
  def tryLock(): Option[Key] | Locker[Key]

  /** Send an item to the listener, consuming the given key.
    * If the listener utilizes a lock, it should be released automatically.
    */
  def complete(item: T)(using Key): Unit

  private def lockAll(): Option[Key] = tryLock() match
    case v: Option[Key] => v
    case l: Locker[Key] => l.lockAll()

  /** Try to lock and complete directly. Returns true if the operation
   *  succeeds, false if the element is not handled.
   */
  def completeNow(data: T): Boolean =
    lockAll().map(complete(data)(using _)).isDefined

/** Locker represents a lock implementation around a `T`. The lock might be nested, in such case
  * locker implementations (except for the given `lockAll`) should not automatically handle and just return the inner lock.
  */
abstract class Locker[T]:
  import scala.annotation.tailrec

  /** Return the uniquely assigned lock number. If the `Locker` wraps an inner lock, the inner lock should have a lower number. */
  def number: Long

  /** Lock the current layer. Returns the locked item (None if it is no longer available), or the next layer of the lock. */
  def lock(): Option[T] | Locker[T]
  /** Release the current layer of the lock. The caller must be holding the lock. */
  def release(): Unit

  /** Attempts to lock continuously until the locker has been completely locked. */
  final def lockAll(): Option[T] =
    lock() match
      case l: Locker[T] =>
        val res = l.lockAll()
        if res == None then release()
        res
      case v: Option[T] => v
