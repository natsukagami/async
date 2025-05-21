package gears.async
import language.experimental.captureChecking

import scala.collection.mutable
import scala.util.Success

import Future.Promise

/** A group of cancellable objects that are completed together. Cancelling the group means cancelling all its
  * uncompleted members.
  */
class CompletionGroup extends Cancellable.Tracking:
  private val members: mutable.Set[Cancellable] = mutable.Set()
  private var canceled: Boolean = false
  private var cancelWait = new CompletionGroup.CancelWait()

  /** Cancel all members */
  def cancel(): Unit =
    synchronized:
      if canceled then Seq.empty
      else
        canceled = true
        members.toSeq
    .foreach(_.cancel())

  /** Wait for all members of the group to complete and unlink themselves. */
  private[async] def waitCompletion()(using Async): Unit =
    synchronized:
      if members.nonEmpty && cancelWait.isDone then cancelWait.reset()
    cancelWait.awaitResult
    unlink()

  /** Add given member to the members set. If the group has already been cancelled, cancels that member immediately. */
  def add(member: Cancellable): Unit =
    val alreadyCancelled = synchronized:
      members += member // Add this member no matter what since we'll wait for it still
      canceled
    if alreadyCancelled then member.cancel()

  /** Remove given member from the members set if it is an element */
  def drop(member: Cancellable): Unit = synchronized:
    members -= member
    if members.isEmpty && !cancelWait.isDone then cancelWait.flush()

  def isCancelled = canceled

object CompletionGroup:
  /** A sentinel group of cancellables that are in fact not linked to any real group. `cancel`, `add`, and `drop` do
    * nothing when called on this group.
    */
  object Unlinked extends CompletionGroup:
    override def cancel(): Unit = ()
    override def waitCompletion()(using Async): Unit = ()
    override def add(member: Cancellable): Unit = ()
    override def drop(member: Cancellable): Unit = ()
  end Unlinked

  private class CancelWait extends Async.OriginalSource[Unit]:
    import caps.unsafe.unsafeAssumePure
    var done = false
    val listeners = mutable.Set[Listener[Unit]]()

    def isDone = done
    def reset() = done = false
    def flush() = synchronized:
      done = true
      listeners.foreach(k => k.completeNow((), this))
      listeners.clear()


    def poll(k: Listener[Unit]^) = 
      if done then { k.completeNow((), this); true }
      else false

    def addListener(k: Listener[Unit]^) = synchronized:
      listeners += k.unsafeAssumePure

    def dropListener(k: Listener[Unit]^) = synchronized:
      listeners -= k.unsafeAssumePure



end CompletionGroup
