package com.github.oceanc.countedis.core.benchmark

import _root_.java.util.concurrent.atomic.AtomicInteger

import com.github.oceanc.countedis.core.{CountSuccessEffect, Cache, CacheLoader, Count}
import com.github.oceanc.countedis.net.{Codec, LgScala}
import com.redis.RedisClient
import org.scalameter.api._
import org.scalameter.picklers.Implicits._

/**
 * @author chengyang
 */
object CacheBenchmark extends Bench.OfflineRegressionReport with Benchmark {

  override val persistor: Persistor = new SerializationPersistor

  val opId = new AtomicInteger(100)
  val opIdPre = "1270016379"
  val id = new AtomicInteger(0)
  val pre = "1000000000000000_someUUID_"
  val expireScnd = 600

  val redis = new RedisClient("127.0.0.1", 6379)
  redis.flushall
  val loader = new CacheLoader {override def load(key: String): Long = 300}
  val cache = new Cache("127.0.0.1", 6379, loader, expireScnd)


  val cdc = Codec.spawn[Array[Count]](-99, Codec.Json, LgScala)
  val loop = Gen.range("loop")(50, 200, 50).cached
  val size = Gen.enumeration("log size")(4, 8, 16, 32).cached
  val logs = size.map(s => new String(cdc.encode((0 until s).map(i => new Count(i + "", 999999)).toArray).get)).cached
  val withLog = Gen.crossProduct(loop, logs).cached


  performance of "Cache exec" in {
    performance of "count transfer" in {
      using(withLog) config (exec.independentSamples -> 1) in { case (lp, log) =>
        loopRun(lp, {
          val key1 = pre + id.incrementAndGet
          val key2 = pre + id.incrementAndGet
          val op = Count.transfer(key1, key2, 10)
          val result = cache.count(opIdPre + opId.incrementAndGet, op, mkLog = true, System.currentTimeMillis, log)
          val effect = result.asInstanceOf[CountSuccessEffect]
          assert(effect.crs(0).key == key1)
          assert(effect.crs(0).value == 290)
          assert(effect.crs(0).argValue == -10)
          assert(effect.crs(0).expireScnd == expireScnd)

          assert(effect.crs(1).key == key2)
          assert(effect.crs(1).value == 310)
          assert(effect.crs(1).argValue == 10)
          assert(effect.crs(1).expireScnd == expireScnd)

          val countLog = cache.popLog(op).get
          val delBackupLog = cache.deleteBackupLog(countLog).get

          assert(countLog.clis.length == effect.crs.length)
          assert(countLog.clis(0).key == effect.crs(0).key)
          assert(countLog.clis(0).value == effect.crs(0).value)
          assert(countLog.clis(0).argValue == effect.crs(0).argValue)
          assert(countLog.clis(1).key == effect.crs(1).key)
          assert(countLog.clis(1).value == effect.crs(1).value)
          assert(countLog.clis(1).argValue == effect.crs(1).argValue)
          assert(countLog.extraLogInfo == log)
          assert(delBackupLog == 1)
        })
      }
    }

    performance of "count transfer ignore log" in {
      using(withLog) config (exec.independentSamples -> 1) in { case (lp, log) =>
        loopRun(lp, {
          val key1 = pre + id.incrementAndGet
          val key2 = pre + id.incrementAndGet
          val op = Count.transfer(key1, key2, 10)
          cache.count(opIdPre + opId.incrementAndGet, op, mkLog = true, System.currentTimeMillis, log)
        })
      }
    }
  }
}
