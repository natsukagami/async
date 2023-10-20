package gears.async

import Future.Promise
import AsyncOperations.sleep

import java.util.concurrent.TimeoutException
import scala.collection.mutable
import scala.concurrent.TimeoutException
import scala.util.{Failure, Success, Try}
import gears.async.Listener
import scala.concurrent.duration._
import scala.annotation.tailrec
import java.util.concurrent.CancellationException

/** 
 * Timer exposes a steady Async.Source of ticks that happens every `tickDuration` milliseconds.
 * Note that the timer does not start ticking until `start` is called (which is a blocking operation, until the timer is cancelled).
 * 
 * You might want to manually `cancel` the timer, so that it gets garbage collected (before the enclosing `Async` scope ends).
*/
class Timer(tickDuration: Duration) extends Cancellable {
  var isCancelled = false

  private class Source extends Async.OriginalSource[this.TimerEvent] {
    def tick() = synchronized {
      listeners.foreach(_.completeNow(TimerEvent.Tick))
    }
    val listeners = mutable.Set[Listener[TimerEvent]]()
    override def poll(k: Listener[TimerEvent]): Boolean = 
      if isCancelled then k.completeNow(TimerEvent.Cancelled) else false // subscribing to a timer always takes you to the next tick
    override def dropListener(k: Listener[TimerEvent]): Unit = listeners -= k
    override protected def addListener(k: Listener[TimerEvent]): Unit = 
      if isCancelled then k.completeNow(TimerEvent.Cancelled)
      else
        Timer.this.synchronized:
          if isCancelled then k.completeNow(TimerEvent.Cancelled)
          else listeners += k
  }
  private val _src = Source()

  /** Ticks of the timer are delivered through this source. Note that ticks are ephemeral. */
  val src: Async.Source[this.TimerEvent] = _src

  def start()(using Async, AsyncOperations): Unit =
    cancellationScope(this):
      loop()

  @tailrec private def loop()(using Async, AsyncOperations): Unit =
    if !isCancelled then
      try
        // println(s"Sleeping at ${new java.util.Date()}, ${isCancelled}, ${this}")
        sleep(tickDuration.toMillis)
      catch
        case _: CancellationException => cancel()
    if !isCancelled then
      _src.tick()
      loop()
    

  override def cancel(): Unit = 
    synchronized { isCancelled = true }
    src.synchronized {
      _src.listeners.foreach(_.completeNow(TimerEvent.Cancelled))
      _src.listeners.clear()
    }

  enum TimerEvent:
    case Tick
    case Cancelled
}

// type TimerRang = Boolean

/** A timer that has to be explicitly started via `start()` to begin counting time.
 *  Can be used only once per instance.
 */
// class StartableTimer(val millis: Long) extends Async.OriginalSource[TimerRang], Cancellable {
//   private enum TimerState(val future: Option[Future[Unit]]):
//     case Ready extends TimerState(None)
//     case Ticking(val f: Future[Unit]) extends TimerState(Some(f))
//     case RangAlready extends TimerState(None)
//     case Cancelled extends TimerState(None)
//   private val waiting: mutable.Set[TimerRang => Boolean] = mutable.Set()
//   @volatile private var state = TimerState.Ready

//     def start()(using Async, AsyncOperations): Unit =
//       state match
//         case TimerState.Cancelled => throw new IllegalStateException("Timers cannot be started after being cancelled.")
//         case TimerState.RangAlready => throw new IllegalStateException("Timers cannot be started after they rang already.")
//         case TimerState.Ticking(_) => throw new IllegalStateException("Timers cannot be started once they have already been started.")
//         case TimerState.Ready =>
//           val f = Future:
//             sleep(millis)
//             var toNotify = List[TimerRang => Boolean]()
//             synchronized:
//               toNotify = waiting.toList
//               waiting.clear()
//               state match
//                 case TimerState.Ticking(_) =>
//                   state = TimerState.RangAlready
//                 case _ =>
//                   toNotify = List()
//             for listener <- toNotify do listener(true)
//           state = TimerState.Ticking(f)

//     def cancel(): Unit =
//       state match
//         case TimerState.Cancelled | TimerState.Ready | TimerState.RangAlready => ()
//         case TimerState.Ticking(f: Future[Unit]) =>
//           f.cancel()
//           val toNotify = synchronized:
//             val ws = waiting.toList
//             waiting.clear()
//             ws
//           for listener <- toNotify do listener(false)
//       state = TimerState.Cancelled

//     def poll(k: Listener[TimerRang]): Boolean =
//       state match
//         case TimerState.Ready | TimerState.Ticking(_) => false
//         case TimerState.RangAlready => k.completeNow(true) ; true
//         case TimerState.Cancelled => k.completeNow(false) ; true

//     def addListener(k: Async.Listener[TimerRang]): Unit = synchronized:
//       waiting += k

//     def dropListener(k: Async.Listener[TimerRang]): Unit = synchronized:
//       waiting -= k
//   }

// /** Exactly like `StartableTimer` except it starts immediately upon instance creation.
//  */
// class Timer(millis: Long)(using Async, AsyncOperations) extends StartableTimer(millis) {
//   this.start()
// }

