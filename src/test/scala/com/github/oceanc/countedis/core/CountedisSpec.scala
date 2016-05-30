package com.github.oceanc.countedis.core

import _root_.java.util.concurrent.atomic.AtomicInteger

import com.redis.RedisClient
import com.redis.serialization.Format.default
import com.redis.serialization.Parse.Implicits._

/**
 * @author chengyang
 */
class CountedisSpec extends UnitSpec {
  val id = new AtomicInteger(0)
  val pre = "1000000000000000_someUUID_"

  val zk = "127.0.0.1:2181"

  val r_1 = "127.0.0.1:6379"
  val r_2 = "127.0.0.1:6380"
  val r_3 = "127.0.0.1:6381"

  val h = "127.0.0.1"
  val port = new AtomicInteger(7890)

  val redis_1 = new RedisClient("127.0.0.1", 6379, database = 0)
  val redis_2 = new RedisClient("127.0.0.1", 6380, database = 0)
  val redis_3 = new RedisClient("127.0.0.1", 6381, database = 0)
  redis_1.flushall
  redis_2.flushall
  redis_3.flushall

  val loader = new CacheLoader {override def load(key: String): Long = 300}
  class MyConsumer(val redis: RedisClient, val k1: String, val k2: String, xlog: String) extends LogConsumer {
    override def consume(log: CountLog): Unit = {
      val valueLog = redis.zrange[String](Cache.HISTORY_VALUE_LOG_PRE + "1").get.head
      val writeLog = redis.zrange[String](Cache.HISTORY_WRITE_LOG_PRE + "1").get.head.split("@")(1)
      val original = redis.llen(Cache.LOG_Q_PREFIX + "1").get
      val backup = redis.lrange[String](Cache.LOG_Q_PREFIX +  Cache.mod(k1) + Cache.LOG_Q_PROCESSING, 0, -1)

      assert(log.extraLogInfo == xlog)
      assert(log.clis(0).key == k1)
      assert(log.clis(0).value == 290)
      assert(log.clis(0).argValue == -10)

      assert(log.clis(1).key == k2)
      assert(log.clis(1).value == 310)
      assert(log.clis(1).argValue == 10)

      assert(backup.isDefined)
      assert(original == 0)
      assert(valueLog.split("@")(1) == "290;310")
      assert(log.toLog.replace(",290", "").replace(",310", "") == writeLog)
    }
  }


  "One countedis instance" should "write data to it's redis" in {
    val k1 = pre + id.incrementAndGet
    val k2 = pre + id.incrementAndGet
    val csmr = new MyConsumer(redis_1, k1, k2, "some log info")

    val c1 = CountedisFactory
      .builder(h, port.incrementAndGet)
      .redisAddress(r_1)
      .zkAddress(zk)
      .cacheLoader(loader)
      .zkSessionTimeoutMs(3000)
      .logConsumer(csmr, 1)
      .build()
    c1.start()

    val t = c1.transfer(k1, k2, 10, "some log info")
    Thread.sleep(200)

    assertValue(t, k1, k2)
    assertRedis(c1, redis_1, k1, k2)

    clean(c1)
  }

  "Three countedis instance" should "leader invoke transfer, leader's redis and follower's redis has same effect." in {
    val k1 = pre + id.incrementAndGet
    val k2 = pre + id.incrementAndGet
    val csmr_1 = new MyConsumer(redis_1, k1, k2, null)
    val csmr_2 = new MyConsumer(redis_2, k1, k2, null)
    val csmr_3 = new MyConsumer(redis_3, k1, k2, null)

    val c1 = CountedisFactory.builder(h, port.incrementAndGet).redisAddress(r_1).zkAddress(zk).cacheLoader(loader).logConsumer(csmr_1, 1).zkSessionTimeoutMs(3000).build()
    val c2 = CountedisFactory.builder(h, port.incrementAndGet).redisAddress(r_2).zkAddress(zk).cacheLoader(loader).logConsumer(csmr_2, 1).zkSessionTimeoutMs(3000).build()
    val c3 = CountedisFactory.builder(h, port.incrementAndGet).redisAddress(r_3).zkAddress(zk).cacheLoader(loader).logConsumer(csmr_3, 1).zkSessionTimeoutMs(3000).build()
    c1.start()
    c2.start()
    c3.start()

    assert(c1.op.fsm.state == Leader)
    assert(c2.op.fsm.state == Follower)
    assert(c3.op.fsm.state == Follower)

    val t = c1.transfer(k1, k2, 10)
    Thread.sleep(200)

    assertValue(t, k1, k2)
    assertRedis(c1, redis_1, k1, k2)
    assertRedis(c2, redis_2, k1, k2)
    assertRedis(c3, redis_3, k1, k2)

    clean(c1, c2, c3)
  }

  it should "follower invoke transfer, leader's redis and follower's redis has same effect." in {
    val k1 = pre + id.incrementAndGet
    val k2 = pre + id.incrementAndGet
    val csmr_1 = new MyConsumer(redis_1, k1, k2, null)
    val csmr_2 = new MyConsumer(redis_2, k1, k2, null)
    val csmr_3 = new MyConsumer(redis_3, k1, k2, null)

    val c1 = CountedisFactory.builder(h, port.incrementAndGet).redisAddress(r_1).zkAddress(zk).cacheLoader(loader).logConsumer(csmr_1, 1).zkSessionTimeoutMs(3000).build()
    val c2 = CountedisFactory.builder(h, port.incrementAndGet).redisAddress(r_2).zkAddress(zk).cacheLoader(loader).logConsumer(csmr_2, 1).zkSessionTimeoutMs(3000).build()
    val c3 = CountedisFactory.builder(h, port.incrementAndGet).redisAddress(r_3).zkAddress(zk).cacheLoader(loader).logConsumer(csmr_3, 1).zkSessionTimeoutMs(3000).build()
    c1.start()
    c2.start()
    c3.start()

    assert(c1.op.fsm.state == Leader)
    assert(c2.op.fsm.state == Follower)
    assert(c3.op.fsm.state == Follower)

    val t = c3.transfer(k1, k2, 10)
    Thread.sleep(200)

    assertValue(t, k1, k2)
    assertRedis(c1, redis_1, k1, k2)
    assertRedis(c2, redis_2, k1, k2)
    assertRedis(c3, redis_3, k1, k2)

    clean(c1, c2, c3)
  }

  it should "load from leader when invoke follower's get method" in {
    val k1 = pre + id.incrementAndGet
    val k2 = pre + id.incrementAndGet
    val csmr_1 = new MyConsumer(redis_1, k1, k2, null)
    val csmr_2 = new MyConsumer(redis_2, k1, k2, null)
    val csmr_3 = new MyConsumer(redis_3, k1, k2, null)

    val c1 = CountedisFactory.builder(h, port.incrementAndGet).redisAddress(r_1).zkAddress(zk).cacheLoader(loader).logConsumer(csmr_1, 1).zkSessionTimeoutMs(3000).build()
    val c2 = CountedisFactory.builder(h, port.incrementAndGet).redisAddress(r_2).zkAddress(zk).cacheLoader(loader).logConsumer(csmr_2, 1).zkSessionTimeoutMs(3000).build()
    val c3 = CountedisFactory.builder(h, port.incrementAndGet).redisAddress(r_3).zkAddress(zk).cacheLoader(loader).logConsumer(csmr_3, 1).zkSessionTimeoutMs(3000).build()
    c1.start()
    c2.start()
    c3.start()

    assert(c1.op.fsm.state == Leader)
    assert(c2.op.fsm.state == Follower)
    assert(c3.op.fsm.state == Follower)

    val v1_1 = c1.get(pre + 1)
    val v2_1 = c2.get(pre + 1)
    val v3_1 = c3.get(pre + 1)

    assert(v1_1 == v2_1)
    assert(v1_1 == v3_1)

    val v1_2 = c1.get(pre + 2)
    val v2_2 = c2.get(pre + 2)
    val v3_2 = c3.get(pre + 2)

    assert(v1_2 == v2_2)
    assert(v1_2 == v3_2)
  }



  def assertValue(t: CountedisResult, k1: String, k2: String): Unit = {
    assert(t.state == 1)
    assert(t.kv.get(k1) == 290)
    assert(t.kv.get(k2) == 310)
  }

  def assertRedis(c: Countedis, redis: RedisClient, k1: String, k2: String): Unit = {
    val v1 = redis.hget[Long](k1, "v").get
    val v2 = redis.hget[Long](k2, "v").get
    val log = c.op.cache.popLog(k1)
    assert(v1 == 290)
    assert(v2 == 310)
    assert(log.isEmpty)
  }

  def clean(c: Countedis*): Unit = {
    redis_1.zremrangebyrank(Cache.HISTORY_VALUE_LOG_PRE + "1")
    redis_1.zremrangebyrank(Cache.HISTORY_WRITE_LOG_PRE + "1")
    redis_2.zremrangebyrank(Cache.HISTORY_VALUE_LOG_PRE + "1")
    redis_2.zremrangebyrank(Cache.HISTORY_WRITE_LOG_PRE + "1")
    redis_3.zremrangebyrank(Cache.HISTORY_VALUE_LOG_PRE + "1")
    redis_3.zremrangebyrank(Cache.HISTORY_WRITE_LOG_PRE + "1")
    c.foreach(x => try {x.close()} catch {case _:Throwable =>})
  }

}
