package com.github.oceanc.countedis.core.benchmark

import _root_.java.util.concurrent.atomic.AtomicInteger

import com.github.oceanc.countedis.core._
import com.redis.RedisClient
import org.scalameter.api._
import org.scalameter.picklers.Implicits._

/**
 * -Xmx2048m -Xms2048m -XX:MaxDirectMemorySize=1024m -Dio.netty.leakDetection.level=advanced
 * @author chengyang
 */
object CountedisBenchmark extends Bench.OfflineRegressionReport with Benchmark {
  override lazy val persistor: Persistor = new SerializationPersistor
  override lazy val executor = LocalExecutor(
    new Executor.Warmer.Default,
    Aggregator.min[Double],
    measurer)


  object Cluster {
    val r_1 = "127.0.0.1:6379"
    val r_2 = "127.0.0.1:6380"
    val r_3 = "127.0.0.1:6381"
    val redis_1 = new RedisClient("127.0.0.1", 6379, database = 0)
    val redis_2 = new RedisClient("127.0.0.1", 6380, database = 0)
    val redis_3 = new RedisClient("127.0.0.1", 6381, database = 0)
    redis_1.flushall
    redis_2.flushall
    redis_3.flushall

    val zk = "127.0.0.1:2181"
    val h = "127.0.0.1"
    val port = new AtomicInteger(7890)

    val loader = new CacheLoader {override def load(key: String): Long = 300}
    class MyConsumer extends LogConsumer {
      override def consume(log: CountLog): Unit = {
        assert(log.extraLogInfo== null || log.extraLogInfo == xlog)
        assert(log.clis(0).value == 290)
        assert(log.clis(0).argValue == -10)

        if (log.clis.length > 1) {
          assert(log.clis(1).value == 310)
          assert(log.clis(1).argValue == 10)
        }
      }
    }

    val csmr_b_1 = new MyConsumer
    val csmr_b_2 = new MyConsumer
    val csmr_b_3 = new MyConsumer
    val c1 = CountedisFactory.builder(h, port.incrementAndGet).redisAddress(r_1).zkAddress(zk).cacheLoader(loader).logConsumer(csmr_b_1, 3).zkSessionTimeoutMs(3000).expireSecond(20).build()
    val c2 = CountedisFactory.builder(h, port.incrementAndGet).redisAddress(r_2).zkAddress(zk).cacheLoader(loader).logConsumer(csmr_b_2, 3).zkSessionTimeoutMs(3000).expireSecond(20).build()
    val c3 = CountedisFactory.builder(h, port.incrementAndGet).redisAddress(r_3).zkAddress(zk).cacheLoader(loader).logConsumer(csmr_b_3, 3).zkSessionTimeoutMs(3000).expireSecond(20).build()
    c1.start()
    c2.start()
    c3.start()
  }


  val xlog =
    """{"locationProvince":"","locationY":"","IP":"10.1.81.223",
      |"locationCity":"","network_desc":"WIFI","locationDistrict":"",
      |"device_id":"497c650b10bc2a6625d2b1fdacf83db013d19a44","sourceFrom":"APP",
      |"wifi":"","size":"375*667","Os_type":"IOS","device_desc":"x86_64",
      |"mac":"497c650b10bc2a6625d2b1fdacf83db013d19a44","locationX":"",
      |"locationAddress":"","network":"WIFI","GPS":"","os_version":"iPhone OS 9.2",
      |"screenSize":"375*667","gmtTime":"Fri, 13 May 2016 06:15:32 GMT"}""".stripMargin
  val id = new AtomicInteger(0)
  val pre = "1000000000000000_someUUID_"
  val sizes = Gen.range("loop")(50, 200, 50).cached
  val parallelismLevels = Gen.enumeration("parallelismLevel")(2, 4, 6)
  val pools = (for (par <- parallelismLevels) yield new collection.parallel.ForkJoinTaskSupport(new concurrent.forkjoin.ForkJoinPool(par))).cached
  val inputs = Gen.crossProduct(sizes, pools)

  performance of "countedis cluster exec" in {
    performance of "leader transfer" in {
      using(inputs) config {
        exec.independentSamples -> 1
        exec.jvmflags -> List("-Xmx2048m", "-Xms2048m", "-XX:MaxDirectMemorySize=1024m", "-Dio.netty.leakDetection.level=advanced")
      } in { case (sz, p) =>
        val pr = (0 until sz).par
        pr.tasksupport = p
        pr.foreach(_ => {
          val k1 = pre + id.incrementAndGet
          val k2 = pre + id.incrementAndGet
          val t = Cluster.c1.transfer(k1, k2, 10)
          assert(t.state == 1)
          assert(t.kv.get(k1) == 290)
          if (t.kv.size > 1) {
            assert(t.kv.get(k2) == 310)
          }
        })
      }
    }

    performance of "leader transfer with log" in {
      using(inputs) config {
        exec.independentSamples -> 1
        exec.jvmflags -> List("-Xmx2048m", "-Xms2048m", "-XX:MaxDirectMemorySize=1024m", "-Dio.netty.leakDetection.level=advanced")
      } in { case (sz, p) =>
        val pr = (0 until sz).par
        pr.tasksupport = p
        pr.foreach(_ => {
          val k1 = pre + id.incrementAndGet
          val k2 = pre + id.incrementAndGet
          val t = Cluster.c1.transfer(k1, k2, 10, xlog)
          assert(t.state == 1)
          assert(t.kv.get(k1) == 290)
          if (t.kv.size > 1) {
            assert(t.kv.get(k2) == 310)
          }
        })
      }
    }
  }
}
