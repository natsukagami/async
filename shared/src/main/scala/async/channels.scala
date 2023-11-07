package gears.async
import scala.collection.mutable
import mutable.{ArrayBuffer, ListBuffer}

import scala.util.{Failure, Success, Try}
import Async.await

import scala.util.control.Breaks.{break, breakable}
import gears.async.Async.Source
import gears.async.Listener.lockBoth
import gears.async.Async.OriginalSource

trait BaseChannel[T]:
  sealed trait Result:
    def mustSent: Unit = this match
      case Sent => ()
      case _ => throw channelClosedException
    def get: Try[T] = this match
      case Read(item) => Success(item)
      case _ => Failure(channelClosedException)
    def mustRead: T = this match
      case Read(item) => item
      case _ => throw channelClosedException

  case class Read(item: T) extends Result
  case object Sent extends Result
  case object Closed extends Result

  type SendResult = Sent.type | Closed.type
  type ReadResult = Read | Closed.type

/** The part of a channel one can send values to. Blocking behavior depends on the implementation.
 *  Note that while sending is a special (potentially) blocking operation similar to await, reading is
 *  done using Async.Sources of channel values.
 */
trait SendableChannel[T] extends BaseChannel[T]:
  def canSend(x: T): Async.Source[SendResult]
  def send(x: T)(using Async): Unit = Async.await(canSend(x)) match
    case Sent => ()
    case Closed => throw channelClosedException
end SendableChannel

/** The part of a channel one can read values from. Blocking behavior depends on the implementation.
 *  Note that while sending is a special (potentially) blocking operation similar to await, reading is
 *  done using Async.Sources of channel values.
 */
trait ReadableChannel[T] extends BaseChannel[T]:
  val canRead: Async.Source[ReadResult]
  def read()(using Async): Try[T] = await(canRead) match
    case Read(item) => Success(item)
    case Closed => Failure(channelClosedException)
end ReadableChannel

trait Channel[T] extends SendableChannel[T], ReadableChannel[T], java.io.Closeable:
  def asSendable(): SendableChannel[T] = this
  def asReadable(): ReadableChannel[T] = this
  def asCloseable(): java.io.Closeable = this

  protected type Reader = Listener[ReadResult]
  protected type Sender = Listener[SendResult]
  protected def complete(item: T, reader: Listener.ListenerLock[ReadResult], sender: Listener.ListenerLock[SendResult]) =
    reader.complete(Read(item))
    sender.complete(Sent)

  private[async] class CellBuf():
    type Cell = Reader | (T, Sender)
    private var reader = 0
    private var sender = 0
    private val pending = mutable.Queue[Cell]()

    def hasReader = reader > 0
    def hasSender = sender > 0
    def nextReader =
      require(reader > 0)
      pending.head.asInstanceOf[Reader]
    def nextSender =
      require(sender > 0)
      pending.head.asInstanceOf[(T, Sender)]
    def dequeue() =
      pending.dequeue()
      if reader > 0 then reader -= 1 else sender -= 1
    def addReader(r: Reader): this.type =
      require(sender == 0)
      reader += 1
      pending.enqueue(r)
      this
    def addSender(item: T, s: Sender): this.type =
      require(reader == 0)
      sender += 1
      pending.enqueue((item, s))
      this
    def dropReader(r: Reader): this.type =
      if reader > 0 then
        if pending.removeFirst(_ == r).isDefined then
          reader -= 1
      this
    def dropSender(item: T, s: Sender): this.type =
      if sender > 0 then
        if pending.removeFirst(_ == (item, s)).isDefined then
          sender -= 1
      this

    def matchReader(r: Reader)(using SuspendSupport): Boolean =
      while hasSender do
        val (item, sender) = nextSender
        lockBoth(r, sender) match
          case Right((read, send)) =>
            Channel.this.complete(item, read, send)
            dequeue()
            return true
          case Left(listener) =>
            if listener == r then
              return true
            else
              dequeue()
      false

    def matchSender(item: T, s: Sender)(using SuspendSupport): Boolean =
      while hasReader do
        val reader = nextReader
        lockBoth(reader, s) match
          case Right((read, send)) =>
            Channel.this.complete(item, read, send)
            dequeue()
            return true
          case Left(listener) =>
            if listener == s then
              return true
            else
              dequeue()
      false


    def cancel() =
      pending.foreach {
        case (_, s) => s.completeNow(Closed)
        case r: Reader => r.completeNow(Closed)
      }
      pending.clear()
      reader = 0
      sender = 0
  end CellBuf
end Channel

/** SyncChannel, sometimes called a rendez-vous channel has the following semantics:
 *  - send to an unclosed channel blocks until a reader willing to accept this value (which is indicated by the
 *    reader's listener returning true after sampling the value) is found and this reader reads the value.
 *  - reading is done via the canRead async source of potential values (wrapped in a Try). Note that only reading
 *    is represented as an async source, sending is a blocking operations that is implemented similarly to how await is implemented.
 */
trait SyncChannel[T] extends Channel[T]

/** BufferedChannel(size: Int) is a version of a channel with an internal value buffer (represented internally as an
 *  array with positive size). It has the following semantics:
 *  - send() if the buffer is not full appends the value to the buffer and returns immediately.
 *  - send() if the buffer is full sleeps until some buffer slot is freed, then writes the value there
 *    and immediately returns.
 *  - reading is done via the canRead async source that awaits the buffer being nonempty and the reader accepting
 *    the first value in the buffer. Because readers can refuse a value, it is possible that many readers await
 *    on canRead while the buffer is non-empty if all of them refused the first value in the buffer. At no point
 *    a reader is allowed to sample/read anything but the first entry in the buffer.
 */
trait BufferedChannel[T] extends Channel[T]

/** This exception is being raised by send()s on closed channel, it is also returned wrapped in Failure
 *  when reading form a closed channel. ChannelMultiplexer sends Failure(ChannelClosedException) to all
 *  subscribers when it receives a close() signal.
 */
class ChannelClosedException extends Exception
private val channelClosedException = ChannelClosedException()

object SyncChannel:
  def apply[T]()(using SuspendSupport): SyncChannel[T] = Impl()

  private class Impl[T](using suspend: SuspendSupport) extends Channel.Impl[T] with SyncChannel[T]:
    def pollRead(r: Reader): Boolean = synchronized:
      // match reader with buffer of senders
      checkClosed(r) || cells.matchReader(r)

    def pollSend(item: T, s: Sender): Boolean = synchronized:
      // match reader with buffer of senders
      checkClosed(s) || cells.matchSender(item, s)
  end Impl
end SyncChannel

object BufferedChannel:
  def apply[T](size: Int = 10)(using SuspendSupport): BufferedChannel[T] = Impl(size)
  private class Impl[T](size: Int)(using SuspendSupport) extends Channel.Impl[T] with BufferedChannel[T]:
    require(size > 0, "Buffered channels must have a buffer size greater than 0")
    val buf = new mutable.Queue[T](size)

    // Match a reader -> check space in buf -> fail
    def pollSend(item: T, s: Sender): Boolean = synchronized:
      checkClosed(s) || cells.matchSender(item, s) || senderToBuf(item, s)

    // Check space in buf -> fail
    // If we can pop from buf -> try to feed a sender
    def pollRead(r: Reader): Boolean = synchronized:
      if checkClosed(r) then
        return true
      if !buf.isEmpty then
        if r.completeNow(Read(buf.head)) then
          buf.dequeue()
          if cells.hasSender then
            val (item, s) = cells.nextSender
            if senderToBuf(item, s) then
              cells.dequeue()
        true
      else false

    // Try to add a sender to the buffer
    def senderToBuf(item: T, s: Sender): Boolean =
      if buf.size < size then
        buf += item
        s.completeNow(Sent)
        true
      else false
  end Impl
end BufferedChannel

object Channel:
  private[async] abstract class Impl[T] extends Channel[T]:
    var isClosed = false
    val cells = CellBuf()
    def pollRead(r: Reader): Boolean
    def pollSend(item: T, s: Sender): Boolean

    def checkClosed(l: Listener[Closed.type]): Boolean =
      if isClosed then
        l.completeNow(Closed)
        true
      else false

    override val canRead: Source[ReadResult] = new Source {
      override def poll(k: Reader): Boolean = pollRead(k)
      override def onComplete(k: Reader): Unit = Impl.this.synchronized:
        if !pollRead(k) then cells.addReader(k)
      override def dropListener(k: Reader): Unit = Impl.this.synchronized:
        if !isClosed then cells.dropReader(k)
    }
    override def canSend(x: T): Source[SendResult] = new Source {
      override def poll(k: Sender): Boolean = pollSend(x, k)
      override def onComplete(k: Sender): Unit = Impl.this.synchronized:
        if !pollSend(x, k) then cells.addSender(x, k)
      override def dropListener(k: Sender): Unit = Impl.this.synchronized:
        if !isClosed then cells.dropSender(x, k)
    }
    override def close(): Unit =
      synchronized:
        if isClosed then return
        isClosed = true
        cells.cancel()
end Channel

/** Channel multiplexer is an object where one can register publisher and subscriber channels.
 *  Internally a multiplexer has a thread that continuously races the set of publishers and once it reads
 *  a value, it sends a copy to each subscriber.
 *
 *  For an unchanging set of publishers and subscribers and assuming that the multiplexer is the only reader of
 *  the publisher channels, every subscriber will receive the same set of messages, in the same order and it will be
 *  exactly all messages sent by the publishers. The only guarantee on the order of the values the subscribers see is
 *  that values from the same publisher will arrive in order.
 *
 *  Channel multiplexer can also be closed, in that case all subscribers will receive Failure(ChannelClosedException)
 *  but no attempt at closing either publishers or subscribers will be made.
 */
trait ChannelMultiplexer[T] extends java.io.Closeable:
  def addPublisher(c: ReadableChannel[T]): Unit
  def removePublisher(c: ReadableChannel[T]): Unit

  def addSubscriber(c: SendableChannel[Try[T]]): Unit

  def removeSubscriber(c: SendableChannel[Try[T]]): Unit
end ChannelMultiplexer


object ChannelMultiplexer:
  private enum Message:
    case Quit, Refresh

  def apply[T]()(using ac: Async): ChannelMultiplexer[T] = new ChannelMultiplexer[T]:
    private var isClosed = false
    private val publishers = ArrayBuffer[ReadableChannel[T]]()
    private val subscribers = ArrayBuffer[SendableChannel[Try[T]]]()
    private val infoChannel: BufferedChannel[Message] = BufferedChannel[Message](1)(using ac.support)
    ac.scheduler.execute { () =>
      var shouldTerminate = false
      var publishersCopy: List[ReadableChannel[T]] = null
      var subscribersCopy: List[SendableChannel[Try[T]]] = null
      while (!shouldTerminate) {
        ChannelMultiplexer.this.synchronized:
          publishersCopy = publishers.toList

        val got = Async.await(Async.race(infoChannel.canRead, Async.race(publishersCopy.map(c => c.canRead.map(_.get)) *)))
        got match {
          case infoChannel.Read(Message.Quit) => {
            ChannelMultiplexer.this.synchronized:
              subscribersCopy = subscribers.toList
            for (s <- subscribersCopy) s.send(Failure(channelClosedException))
            shouldTerminate = true
          }
          case infoChannel.Read(Message.Refresh) => ()
          case infoChannel.Closed => shouldTerminate = true // ?
          case v: Try[T] => {
            ChannelMultiplexer.this.synchronized:
              subscribersCopy = subscribers.toList
            var c = 0
            for (s <- subscribersCopy) {
              c += 1
              s.send(v)
            }
          }
        }
      }
    }

    override def close(): Unit =
      var shouldStop = false
      ChannelMultiplexer.this.synchronized:
        if (!isClosed) {
          isClosed = true
          shouldStop = true
        }
      if (shouldStop) infoChannel.send(Message.Quit)

    override def removePublisher(c: ReadableChannel[T]): Unit =
      ChannelMultiplexer.this.synchronized:
        if (isClosed) throw channelClosedException
        publishers -= c
      infoChannel.send(Message.Refresh)

    override def removeSubscriber(c: SendableChannel[Try[T]]): Unit =
      ChannelMultiplexer.this.synchronized:
        if (isClosed) throw channelClosedException
        subscribers -= c

    override def addPublisher(c: ReadableChannel[T]): Unit =
      ChannelMultiplexer.this.synchronized:
        if (isClosed) throw channelClosedException
        publishers += c
      infoChannel.send(Message.Refresh)

    override def addSubscriber(c: SendableChannel[Try[T]]): Unit =
      ChannelMultiplexer.this.synchronized:
        if (isClosed) throw channelClosedException
        subscribers += c

end ChannelMultiplexer

