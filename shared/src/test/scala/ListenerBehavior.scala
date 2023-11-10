import gears.async.Async.race
import gears.async.Future
import gears.async.Future.Promise
import gears.async.Async
import gears.async.Listener
import gears.async.Locker
import gears.async.default.given
import scala.util.Success

class ListenerBehavior extends munit.FunSuite:
  test("race two futures"):
    val prom1 = Promise[Unit]()
    val prom2 = Promise[Unit]()
    Async.blocking:
      val raced = race(Future { prom1.future.value ; 10 }, Future { prom2.future.value ; 20 })
      assert(!raced.poll(Listener.acceptingListener(x => fail(s"race uncomplete $x"))))
      prom1.complete(Success(()))
      assertEquals(Async.await(raced).get, 10)

  test("lock two listeners"):
    val listener1 = Listener.acceptingListener[Int](x => assertEquals(x, 1))
    val listener2 = Listener.acceptingListener[Int](x => assertEquals(x, 2))
    val (lock1, lock2) = Listener.lockBoth(listener1, listener2)
    listener1.complete(1)(using lock1)
    listener2.complete(2)(using lock2)

  test("lock two listeners, one fails"):
    var listener1Locked = false
    val listener1 = new Listener[Nothing]:
      type Key = Unit
      def tryLock() = new Locker[Key]:
        def number = 1
        def lock() =
          listener1Locked = true
          Some(())
        def release() = listener1Locked = false
      def complete(data: Nothing)(using Key): Unit =
        fail("should not succeed")
        listener1Locked = false
    val listener2 = new Listener[Nothing]:
      def tryLock() = None
      def complete(data: Nothing)(using Key): Unit =
        fail("should not succeed")


    assertEquals(Listener.lockBoth(listener1, listener2).asInstanceOf[Listener[?]], listener2)
    assert(!listener1Locked)

    assertEquals(Listener.lockBoth(listener2, listener1).asInstanceOf[Listener[?]], listener2)
    assert(!listener1Locked)

  test("lock two races"):
    val source1 = TSource()
    val source2 = TSource()

    Async.race(source1).onComplete(Listener.acceptingListener[Int](x => assertEquals(x, 1)))
    Async.race(source2).onComplete(Listener.acceptingListener[Int](x => assertEquals(x, 2)))

    val (l1, l2) = (source1.listener.get, source2.listener.get)
    val (lock1, lock2) = Listener.lockBoth(l1, l2)
    l1.complete(1)(using lock1)
    l2.complete(2)(using lock2)

  test("lock two races in reverse order"):
    val source1 = TSource()
    val source2 = TSource()

    Async.race(source1).onComplete(Listener.acceptingListener[Int](x => assertEquals(x, 1)))
    Async.race(source2).onComplete(Listener.acceptingListener[Int](x => assertEquals(x, 2)))

    val (l1, l2) = (source1.listener.get, source2.listener.get)
    val (lock2, lock1) = Listener.lockBoth(l2, l1)
    l1.complete(1)(using lock1)
    l2.complete(2)(using lock2)

  test("lock two nested races"):
    val source1 = TSource()
    val source2 = TSource()

    val race1 = Async.race(source1)
    Async.race(Async.race(source2)).onComplete(Listener.acceptingListener[Int](x => assertEquals(x, 2)))
    Async.race(race1).onComplete(Listener.acceptingListener[Int](x => assertEquals(x, 1)))

    val (l1, l2) = (source1.listener.get, source2.listener.get)
    val (lock1, lock2) = Listener.lockBoth(l1, l2)
    l1.complete(1)(using lock1)
    l2.complete(2)(using lock2)

  test("lock two nested races in reverse order"):
    val source1 = TSource()
    val source2 = TSource()

    val race1 = Async.race(source1)
    Async.race(Async.race(source2)).onComplete(Listener.acceptingListener[Int](x => assertEquals(x, 2)))
    Async.race(race1).onComplete(Listener.acceptingListener[Int](x => assertEquals(x, 1)))

    val (l1, l2) = (source1.listener.get, source2.listener.get)
    val (lock2, lock1) = Listener.lockBoth(l2, l1)
    l1.complete(1)(using lock1)
    l2.complete(2)(using lock2)

  private class TSource extends Async.Source[Int]:
    var listener: Option[Listener[Int]] = None
    def poll(k: Listener[Int]): Boolean = ???
    def onComplete(k: Listener[Int]): Unit =
      assert(listener.isEmpty)
      listener = Some(k)
    def dropListener(k: Listener[Int]): Unit =
      assertEquals(k, listener.get)
      listener = None
