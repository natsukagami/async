//> using scala 3.3.3
//> using dep ch.epfl.lamp:::gears:0.2.0-RC4+11-14088d45+20240627-1529-SNAPSHOT
//> using nativeVersion 0.5.5-SNAPSHOT

@main def main() =
  val total = 100_000L
  val parallelism = 5000L
  Async.blocking:
    val sleepy =
      val timer = Timer(1.second)
      Future { timer.run() }
      timer.src
    val k = AtomicInteger(0)
    def compute(using Async) =
      sleepy.awaitResult
      k.incrementAndGet()
    val collector = MutableCollector((1L to parallelism).map(_ => Future { compute })*)
    var sum = 0L
    for i <- parallelism + 1 to total do
      sum += collector.results.read().right.get.await
      collector += Future { compute }
    for i <- 1L to parallelism do sum += collector.results.read().right.get.await
    assertEquals(sum, total * (total + 1) / 2)
