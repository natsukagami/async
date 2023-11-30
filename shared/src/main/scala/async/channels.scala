package gears.async
import scala.collection.mutable
import mutable.{ArrayBuffer, ListBuffer}

import scala.util.{Failure, Success, Try}
import Async.await

import scala.util.control.Breaks.{break, breakable}
import gears.async.Async.Source
import gears.async.listeners.lockBoth
import gears.async.Async.OriginalSource
import gears.async.Listener.acceptingListener

/** The part of a channel one can send values to. Blocking behavior depends on the implementation.
  */
trait SendableChannel[-T]:
  /** Create an [[Async.Source]] representing the send action of value [[x]]. Note that *each* listener attached to and
    * accepting a [[Sent]] value corresponds to [[x]] being sent once.
    *
    * To create an [[Async.Source]] that sends the item exactly once regardless of listeners attached, wrap the [[send]]
    * operation inside a [[gears.async.Future]].
    */
  def sendSource(x: T): Async.Source[Unit]

  /** Send [[x]] over the channel, blocking (asynchronously with [[Async]]) until the item has been sent or, if the
    * channel is buffered, queued. Throws [[ChannelClosedException]] if the channel was closed.
    */
  def send(x: T)(using Async): Unit = Async.await(sendSource(x)).get
end SendableChannel

/** The part of a channel one can read values from. Blocking behavior depends on the implementation.
  */
trait ReadableChannel[+T]:
  /** An [[Async.Source]] corresponding to items being sent over the channel. Note that *each* listener attached to and
    * accepting a [[Read]] value corresponds to one value received over the channel.
    *
    * To create an [[Async.Source]] that reads *exactly one* item regardless of listeners attached, wrap the [[read]]
    * operation inside a [[gears.async.Future]].
    */
  val readSource: Async.Source[T]

  /** Read an item from the channel, blocking (asynchronously with [[Async]]) until the item has been received. Returns
    * `Failure(ChannelClosedException)` if the channel was closed.
    */
  def read()(using Async): Try[T] = await(readSource)
end ReadableChannel

/** A channel that is both sendable and closeable. */
type SendCloseableChannel[-T] = SendableChannel[T] & java.io.Closeable

/** A generic channel that can be sent to, received from and closed. */
trait Channel[T] extends SendableChannel[T], ReadableChannel[T], java.io.Closeable:
  def asSendable: SendableChannel[T] = this
  def asReadable: ReadableChannel[T] = this
  def asCloseable: java.io.Closeable = this

  protected type Reader = Listener[T]
  protected type Sender = Listener[Unit]
end Channel

/** SyncChannel, sometimes called a rendez-vous channel has the following semantics:
  *   - `send` to an unclosed channel blocks until a reader commits to receiving the value (via successfully locking).
  */
trait SyncChannel[T] extends Channel[T]

/** BufferedChannel(size: Int) is a version of a channel with an internal value buffer (represented internally as an
  * array with positive size). It has the following semantics:
  *   - `send` if the buffer is not full appends the value to the buffer and success immediately.
  *   - `send` if the buffer is full blocks until some buffer slot is freed and assigned to this sender.
  */
trait BufferedChannel[T] extends Channel[T]

/** UnboundedChannel are buffered channels that does not bound the number of items in the channel. In other words, the
  * buffer is treated as never being full and will expand as needed.
  */
trait UnboundedChannel[T] extends BufferedChannel[T]:
  /** Send the item immediately. Throws [[ChannelClosedException]] if the channel is closed. */
  def sendImmediately(x: T): Unit

/** This exception is being raised by [[Channel.send]] on closed [[Channel]], it is also returned wrapped in `Failure`
  * when reading form a closed channel. [[ChannelMultiplexer]] sends `Failure(ChannelClosedException)` to all
  * subscribers when it receives a `close()` signal.
  */
case class ChannelClosedException(channel: java.io.Closeable) extends Exception

object SyncChannel:
  def apply[T](): SyncChannel[T] = Impl()

  private class Impl[T] extends Channel.Impl[T] with SyncChannel[T]:
    override def pollRead(r: Reader): Boolean = synchronized:
      // match reader with buffer of senders
      checkClosed(readSource, r) || cells.matchReader(r)

    override def pollSend(src: CanSend, s: Sender): Boolean = synchronized:
      // match reader with buffer of senders
      checkClosed(src, s) || cells.matchSender(src, s)
  end Impl
end SyncChannel

object BufferedChannel:
  /** Create a new buffered channel with the given buffer size. */
  def apply[T](size: Int = 10): BufferedChannel[T] = Impl(size)
  private class Impl[T](size: Int) extends Channel.Impl[T] with BufferedChannel[T]:
    require(size > 0, "Buffered channels must have a buffer size greater than 0")
    val buf = new mutable.Queue[T](size)

    // Match a reader -> check space in buf -> fail
    override def pollSend(src: CanSend, s: Sender): Boolean = synchronized:
      checkClosed(src, s) || cells.matchSender(src, s) || senderToBuf(src, s)

    // Check space in buf -> fail
    // If we can pop from buf -> try to feed a sender
    override def pollRead(r: Reader): Boolean = synchronized:
      if checkClosed(readSource, r) then return true
      if !buf.isEmpty then
        if r.completeNow(buf.head, readSource) then
          buf.dequeue()
          if cells.hasSender then
            val (src, s) = cells.nextSender
            if senderToBuf(src, s) then cells.dequeue()
        true
      else false

    // Try to add a sender to the buffer
    def senderToBuf(src: CanSend, s: Sender): Boolean =
      if buf.size < size then
        buf += src.item
        s.completeNow((), src)
        true
      else false
  end Impl
end BufferedChannel

object UnboundedChannel:
  def apply[T](): UnboundedChannel[T] = Impl[T]()

  private final class Impl[T]() extends Channel.Impl[T] with UnboundedChannel[T] {
    val buf = new mutable.Queue[T]()

    override def sendImmediately(x: T): Unit =
      var result: Try[Unit] = null
      pollSend(CanSend(x), acceptingListener((r, _) => result = r))
      result.get

    override def pollRead(r: Reader): Boolean = synchronized:
      if checkClosed(readSource, r) then true
      else if !buf.isEmpty then
        if r.completeNow(buf.head, readSource) then
          // there are never senders in the cells
          buf.dequeue()
        true
      else false

    override def pollSend(src: CanSend, s: Sender): Boolean = synchronized:
      checkClosed(src, s) || cells.matchSender(src, s) || {
        buf += src.item
        s.completeNow((), src)
        true
      }
  }
end UnboundedChannel

object Channel:
  private[async] abstract class Impl[T] extends Channel[T]:
    var isClosed = false
    val cells = CellBuf()
    def pollRead(r: Reader): Boolean
    def pollSend(src: CanSend, s: Sender): Boolean

    inline def channelClosedException = ChannelClosedException(this)

    protected final def checkClosed[T](src: Async.Source[T], l: Listener[T]): Boolean =
      if isClosed then
        l.completeNow(Failure(channelClosedException), src)
        true
      else false

    override final val readSource: Source[T] = new Source {
      override def poll(k: Reader): Boolean = pollRead(k)
      override def onComplete(k: Reader): Unit = Impl.this.synchronized:
        if !pollRead(k) then cells.addReader(k)
      override def dropListener(k: Reader): Unit = Impl.this.synchronized:
        if !isClosed then cells.dropReader(k)
    }
    override final def sendSource(x: T): Source[Unit] = CanSend(x)
    override final def close(): Unit =
      synchronized:
        if isClosed then return
        isClosed = true
        cells.cancel()

    protected final def complete(src: CanSend, reader: Reader, sender: Sender) =
      reader.complete(src.item, readSource)
      sender.complete((), src)

    protected final case class CanSend(item: T) extends OriginalSource[Unit] {
      override def poll(k: Listener[Unit]): Boolean = pollSend(this, k)
      override protected def addListener(k: Listener[Unit]): Unit = Impl.this.synchronized:
        if !pollSend(this, k) then cells.addSender(this, k)
      override def dropListener(k: Listener[Unit]): Unit = Impl.this.synchronized:
        if !isClosed then cells.dropSender(item, k)
    }

    private[async] class CellBuf():
      type Cell = Reader | (CanSend, Sender)
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
        pending.head.asInstanceOf[(CanSend, Sender)]
      def dequeue() =
        pending.dequeue()
        if reader > 0 then reader -= 1 else sender -= 1
      def addReader(r: Reader): this.type =
        require(sender == 0)
        reader += 1
        pending.enqueue(r)
        this
      def addSender(src: CanSend, s: Sender): this.type =
        require(reader == 0)
        sender += 1
        pending.enqueue((src, s))
        this
      def dropReader(r: Reader): this.type =
        if reader > 0 then if pending.removeFirst(_ == r).isDefined then reader -= 1
        this
      def dropSender(item: T, s: Sender): this.type =
        if sender > 0 then if pending.removeFirst(_ == (item, s)).isDefined then sender -= 1
        this

      def matchReader(r: Reader): Boolean =
        while hasSender do
          val (canSend, sender) = nextSender
          lockBoth(readSource, canSend)(r, sender) match
            case Listener.Locked =>
              Impl.this.complete(canSend, r, sender)
              dequeue()
              return true
            case listener: (r.type | sender.type) =>
              if listener == r then return true
              else dequeue()
        false

      def matchSender(src: CanSend, s: Sender): Boolean =
        while hasReader do
          val reader = nextReader
          lockBoth(readSource, src)(reader, s) match
            case Listener.Locked =>
              Impl.this.complete(src, reader, s)
              dequeue()
              return true
            case listener: (reader.type | s.type) =>
              if listener == s then return true
              else dequeue()
        false

      def cancel() =
        pending.foreach {
          case (src, s)  => s.completeNow(Failure(channelClosedException), src)
          case r: Reader => r.completeNow(Failure(channelClosedException), readSource)
        }
        pending.clear()
        reader = 0
        sender = 0
    end CellBuf
  end Impl
end Channel

/** Channel multiplexer is an object where one can register publisher and subscriber channels. Internally a multiplexer
  * has a thread that continuously races the set of publishers and once it reads a value, it sends a copy to each
  * subscriber.
  *
  * For an unchanging set of publishers and subscribers and assuming that the multiplexer is the only reader of the
  * publisher channels, every subscriber will receive the same set of messages, in the same order and it will be exactly
  * all messages sent by the publishers. The only guarantee on the order of the values the subscribers see is that
  * values from the same publisher will arrive in order.
  *
  * Channel multiplexer can also be closed, in that case all subscribers will receive Failure(ChannelClosedException)
  * but no attempt at closing either publishers or subscribers will be made.
  */
trait ChannelMultiplexer[T] extends java.io.Closeable:
  def addPublisher(c: ReadableChannel[T]): Unit
  def removePublisher(c: ReadableChannel[T]): Unit

  def addSubscriber(c: SendCloseableChannel[T]): Unit

  def removeSubscriber(c: SendCloseableChannel[T]): Unit
end ChannelMultiplexer

object ChannelMultiplexer:
  private enum Message:
    case Quit, Refresh

  def apply[T]()(using Async): ChannelMultiplexer[T] = new ChannelMultiplexer[T]:
    private var isClosed = false
    private val publishers = ArrayBuffer[ReadableChannel[T]]()
    private val subscribers = ArrayBuffer[SendCloseableChannel[T]]()
    private val infoChannel: BufferedChannel[Message] = BufferedChannel[Message](1)
    Future:
      var shouldTerminate = false
      var publishersCopy: List[ReadableChannel[T]] = null
      var subscribersCopy: List[SendCloseableChannel[T]] = null
      while (!shouldTerminate) {
        ChannelMultiplexer.this.synchronized:
          publishersCopy = publishers.toList

        val got = Async.await(Async.race(infoChannel.readSource, Async.race(publishersCopy.map(_.readSource)*)))
        got match
          case Success(Message.Quit) => {
            ChannelMultiplexer.this.synchronized:
              subscribersCopy = subscribers.toList
            for (s <- subscribersCopy) s.close()
            shouldTerminate = true
          }
          case Success(Message.Refresh)                               => ()
          case Failure(ChannelClosedException(c)) if c == infoChannel => shouldTerminate = true // ?
          case Failure(ChannelClosedException(c)) =>
            publishersCopy.foreach(p => if p == c then removePublisher(p))
          case Success(v: T) =>
            ChannelMultiplexer.this.synchronized:
              subscribersCopy = subscribers.toList
            var c = 0
            for s <- subscribersCopy do
              c += 1
              s.send(v)
          case Failure(exc) => throw exc // should not happen
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
        if (isClosed) throw ChannelClosedException(this)
        publishers -= c
      infoChannel.send(Message.Refresh)

    override def removeSubscriber(c: SendCloseableChannel[T]): Unit =
      ChannelMultiplexer.this.synchronized:
        if (isClosed) throw ChannelClosedException(this)
        subscribers -= c

    override def addPublisher(c: ReadableChannel[T]): Unit =
      ChannelMultiplexer.this.synchronized:
        if (isClosed) throw ChannelClosedException(this)
        publishers += c
      infoChannel.send(Message.Refresh)

    override def addSubscriber(c: SendCloseableChannel[T]): Unit =
      ChannelMultiplexer.this.synchronized:
        if (isClosed) throw ChannelClosedException(this)
        subscribers += c
  end apply

end ChannelMultiplexer
