package com.github.oceanc.countedis.core

import _root_.java.util.concurrent.atomic.AtomicInteger

import com.redis.RedisClient
import com.redis.serialization.Format.default
import com.redis.serialization.Parse.Implicits._

/**
 * @author chengyang
 */
class CacheSpec extends UnitSpec {
  val opId = new AtomicInteger(100)
  val opIdPre = "1270017890"
  val id = new AtomicInteger(0)
  val pre = "1000000000000000_someUUID_"
  val expireScnd = 5
  val extraLog = "{json-key: json-value, other: [other-value]}"

  val redis = new RedisClient("127.0.0.1", 6379)
  redis.flushall

  val loader = new CacheLoader {override def load(key: String): Long = 300}
  val cache = new Cache("127.0.0.1", 6379, loader, expireScnd)
  cache.init()

  "A Cache" should "exec transfer without log" in {
    val wlogId = opIdPre + opId.incrementAndGet
    val key1 = pre + id.incrementAndGet
    val key2 = pre + id.incrementAndGet

    val op = Count.transfer(key1, key2, 10)
    val time = System.currentTimeMillis
    val result = cache.count(wlogId, op, mkLog = false, time, null)
    val effect = result.asInstanceOf[CountSuccessEffect]

    val v1 = cache.get(key1).get
    val v2 = cache.get(key2).get
    val log = cache.popLog(op)
    val existWriteLog = cache.hasExecuted(wlogId, op, time, null).get

    assert(existWriteLog.head == 290)
    assert(existWriteLog(1) == 310)
    assert(v1 == 290)
    assert(v2 == 310)
    assert(effect.crs.length == 2)

    assert(effect.crs(0).key == key1)
    assert(effect.crs(0).value == 290)
    assert(effect.crs(0).argValue == -10)
    assert(effect.crs(0).expireScnd == expireScnd)

    assert(effect.crs(1).key == key2)
    assert(effect.crs(1).value == 310)
    assert(effect.crs(1).argValue == 10)
    assert(effect.crs(1).expireScnd == expireScnd)
    assert(log.isEmpty)
  }

  it should "exec transfer with log" in {
    val wlogId = opIdPre + opId.incrementAndGet
    val key1 = pre + id.incrementAndGet
    val key2 = pre + id.incrementAndGet

    val op = Count.transfer(key1, key2, 10)
    val time = System.currentTimeMillis
    val result = cache.count(wlogId, op, mkLog = true, time, extraLog)
    val effect = result.asInstanceOf[CountSuccessEffect]
    val existWriteLog = cache.hasExecuted(wlogId, op, time, extraLog).get

    val v1 = cache.get(key1).get
    val v2 = cache.get(key2).get
    val countLog = cache.popLog(op).get
    val delBackupLog = cache.deleteBackupLog(countLog).get

    assert(existWriteLog.head == 290)
    assert(existWriteLog(1) == 310)
    assert(v1 == 290)
    assert(v2 == 310)
    assert(effect.crs.length == 2)

    assert(effect.crs(0).key == key1)
    assert(effect.crs(0).value == 290)
    assert(effect.crs(0).argValue == -10)
    assert(effect.crs(0).expireScnd == expireScnd)

    assert(effect.crs(1).key == key2)
    assert(effect.crs(1).value == 310)
    assert(effect.crs(1).argValue == 10)
    assert(effect.crs(1).expireScnd == expireScnd)
    assert(delBackupLog == 1)

    assert(countLog.clis.length      == effect.crs.length)
    assert(countLog.clis(0).key      == effect.crs(0).key)
    assert(countLog.clis(0).value    == effect.crs(0).value)
    assert(countLog.clis(0).argValue == effect.crs(0).argValue)
    assert(countLog.clis(1).key      == effect.crs(1).key)
    assert(countLog.clis(1).value    == effect.crs(1).value)
    assert(countLog.clis(1).argValue == effect.crs(1).argValue)
    assert(countLog.extraLogInfo     == extraLog)
  }

  it should "exec incr without log" in {
    val wlogId = opIdPre + opId.incrementAndGet
    val key1 = pre + id.incrementAndGet
    val op = Count.incrBy(key1, 10)
    val time = System.currentTimeMillis
    val result = cache.count(wlogId, op, mkLog = false, time, null)
    val effect = result.asInstanceOf[CountSuccessEffect]
    val existWriteLog = cache.hasExecuted(wlogId, op, time, null).get

    val v1 = cache.get(key1).get
    val log = cache.popLog(op)

    assert(existWriteLog.head == 310)
    assert(v1 == 310)
    assert(effect.crs.length == 1)

    assert(effect.crs(0).key == key1)
    assert(effect.crs(0).value == 310)
    assert(effect.crs(0).argValue == 10)
    assert(effect.crs(0).expireScnd == expireScnd)
    assert(log.isEmpty)
  }

  it should "exec incr with log" in {
    val wlogId = opIdPre + opId.incrementAndGet
    val key1 = pre + id.incrementAndGet
    val op = Count.incrBy(key1, 10)
    val time = System.currentTimeMillis
    val result = cache.count(wlogId, op, mkLog = true, time, extraLog)
    val effect = result.asInstanceOf[CountSuccessEffect]
    val existWriteLog = cache.hasExecuted(wlogId, op, time, extraLog).get

    val v1 = cache.get(key1).get
    val countLog = cache.popLog(op).get
    val delBackupLog = cache.deleteBackupLog(countLog).get

    assert(existWriteLog.head == 310)
    assert(v1 == 310)
    assert(effect.crs.length == 1)

    assert(effect.crs(0).key == key1)
    assert(effect.crs(0).value == 310)
    assert(effect.crs(0).argValue == 10)
    assert(effect.crs(0).expireScnd == expireScnd)
    assert(delBackupLog == 1)

    assert(countLog.clis.length      == effect.crs.length)
    assert(countLog.clis(0).key      == effect.crs(0).key)
    assert(countLog.clis(0).value    == effect.crs(0).value)
    assert(countLog.clis(0).argValue == effect.crs(0).argValue)
    assert(countLog.extraLogInfo     == extraLog)
  }

  it should "exec decr without log" in {
    val wlogId = opIdPre + opId.incrementAndGet
    val key1 = pre + id.incrementAndGet
    val op = Count.decrBy(key1, 10)
    val time = System.currentTimeMillis
    val result = cache.count(wlogId, op, mkLog = false, time, null)
    val effect = result.asInstanceOf[CountSuccessEffect]
    val existWriteLog = cache.hasExecuted(wlogId, op, time, null).get

    val v1 = cache.get(key1).get
    val log = cache.popLog(op)

    assert(existWriteLog.head == 290)
    assert(v1 == 290)
    assert(effect.crs.length == 1)

    assert(effect.crs(0).key == key1)
    assert(effect.crs(0).value == 290)
    assert(effect.crs(0).argValue == -10)
    assert(effect.crs(0).expireScnd == expireScnd)
    assert(log.isEmpty)
  }

  it should "exec decr with log" in {
    val wlogId = opIdPre + opId.incrementAndGet
    val key1 = pre + id.incrementAndGet
    val op = Count.decrBy(key1, 10)
    val time = System.currentTimeMillis
    val result = cache.count(wlogId, op, mkLog = true, time, extraLog)
    val effect = result.asInstanceOf[CountSuccessEffect]
    val existWriteLog = cache.hasExecuted(wlogId, op, time, extraLog).get

    val v1 = cache.get(key1).get
    val countLog = cache.popLog(op).get
    val delBackupLog = cache.deleteBackupLog(countLog).get

    assert(existWriteLog.head == 290)
    assert(v1 == 290)
    assert(effect.crs.length == 1)

    assert(effect.crs(0).key == key1)
    assert(effect.crs(0).value == 290)
    assert(effect.crs(0).argValue == -10)
    assert(effect.crs(0).expireScnd == expireScnd)
    assert(delBackupLog == 1)

    assert(countLog.clis.length      == op.length)
    assert(countLog.clis(0).key      == op.head.key)
    assert(countLog.clis(0).value    == effect.crs(0).value)
    assert(countLog.clis(0).argValue == effect.crs(0).argValue)
    assert(countLog.extraLogInfo     == extraLog)
  }

  it should "effect transfer without log" in {
    val wlogId = opIdPre + opId.incrementAndGet
    val key1 = pre + id.incrementAndGet
    val key2 = pre + id.incrementAndGet
    val lastSeq = 7

    val coss = Array(CountResult(key1, 290, -10, lastSeq, expireScnd), CountResult(key2, 310, 10, lastSeq, expireScnd))
    val timestamp = System.currentTimeMillis
    val ce = new CountSuccessEffect(wlogId, coss, timestamp, mkLog = false, null)
    val e = cache.effect(ce)
    val existWriteLog = cache.hasExecuted(ce).get

    val v1 = cache.get(key1).get
    val v2 = cache.get(key2).get
    val countLog = cache.popLog(ce)

    assert(e)
    assert(existWriteLog.head == 290)
    assert(existWriteLog(1) == 310)
    assert(v1 == 290)
    assert(v2 == 310)
    assert(countLog.isEmpty)
  }

  it should "effect transfer with log" in {
    val wlogId = opIdPre + opId.incrementAndGet
    val key1 = pre + id.incrementAndGet
    val key2 = pre + id.incrementAndGet
    val lastSeq = 8

    val coss = Array(CountResult(key1, 290, -10, lastSeq, expireScnd), CountResult(key2, 310, 10, lastSeq, expireScnd))
    val timestamp = System.currentTimeMillis
    val ce = new CountSuccessEffect(wlogId, coss, timestamp, mkLog = true, extraLog)
    val e = cache.effect(ce)
    val existWriteLog = cache.hasExecuted(ce).get

    val v1 = cache.get(key1).get
    val v2 = cache.get(key2).get
    val countLog = cache.popLog(ce).get
    val delBackupLog = cache.deleteBackupLog(ce).get

    assert(e)
    assert(existWriteLog.head == 290)
    assert(existWriteLog(1) == 310)
    assert(v1 == 290)
    assert(v2 == 310)
    assert(delBackupLog == 1)

    assert(countLog.clis.length      == coss.length)
    assert(countLog.clis(0).key      == coss(0).key)
    assert(countLog.clis(0).value    == coss(0).value)
    assert(countLog.clis(0).argValue == coss(0).argValue)

    assert(countLog.clis(1).key      == coss(1).key)
    assert(countLog.clis(1).value    == coss(1).value)
    assert(countLog.clis(1).argValue == coss(1).argValue)
    assert(countLog.extraLogInfo     == extraLog)
  }

  it should "rollback transfer" in {
    val wlogId = opIdPre + opId.incrementAndGet
    val key1 = pre + id.incrementAndGet
    val key2 = pre + id.incrementAndGet
    val lastSeq = 9

    val coss = Array(CountResult(key1, 290, -10, lastSeq, expireScnd), CountResult(key2, 310, 10, lastSeq, expireScnd))
    val timestamp = System.currentTimeMillis
    val ce = new CountSuccessEffect(wlogId, coss, timestamp, mkLog = true, extraLog)
    val log = ce.convertToLog.toLog
    val wlog = ce.convertToWriteLog
    val e = cache.effect(ce)
    val existWriteLog = cache.hasExecuted(ce).get

    val v1 = cache.get(key1).get
    val v2 = cache.get(key2).get

    val wLogs = redis.zrange[String](Cache.HISTORY_WRITE_LOG_PRE + "1").get
    val bizLogs  = redis.lrange[String](Cache.LOG_Q_PREFIX + Cache.mod(key1), 0, -1).get

    assert(e)
    assert(existWriteLog.head == 290)
    assert(existWriteLog(1) == 310)
    assert(v1 == 290)
    assert(v2 == 310)
    assert(wLogs.contains(wlog))
    assert(bizLogs.contains(Some(log)))

    val backSeq = lastSeq - 1
    val roll = cache.rollback(backSeq)
    val origV1 = cache.get(key1).get
    val origV2 = cache.get(key2).get
    val key1_s = redis.hget[Long](key1, "s").get
    val key2_s = redis.hget[Long](key2, "s").get
    val origWLogs = redis.zrange[String](Cache.HISTORY_WRITE_LOG_PRE + "1").get
    val origBizLogs  = redis.lrange[String](Cache.LOG_Q_PREFIX + Cache.mod(key1), 0, -1).get
    val seq = redis.get[Long](Cache.QUORUM_SEQUENCER).get

    assert(roll)
    assert(origV1 == 300)
    assert(origV2 == 300)
    assert(key1_s == backSeq)
    assert(key2_s == backSeq)
    assert(!origWLogs.contains(wlog))
    assert(!origBizLogs.contains(Some(log)))
    assert(seq == backSeq)
  }

  it should "rollback incr" in {
    val wlogId = opIdPre + opId.incrementAndGet
    val key1 = pre + id.incrementAndGet
    val lastSeq = 10

    val coss = Array(CountResult(key1, 310, 10, lastSeq, expireScnd))
    val ce = new CountSuccessEffect(wlogId, coss, System.currentTimeMillis, mkLog = false, null)
    val log = ce.convertToLog.toLog
    val wlog = ce.convertToWriteLog
    val e = cache.effect(ce)
    val existWriteLog = cache.hasExecuted(ce).get

    val v1 = cache.get(key1).get
    val wLogs = redis.zrange[String](Cache.HISTORY_WRITE_LOG_PRE + "1").get
    val bizLogs  = redis.lrange[String](Cache.LOG_Q_PREFIX + Cache.mod(key1), 0, -1).get

    assert(e)
    assert(existWriteLog.head == 310)
    assert(v1 == 310)
    assert(wLogs.contains(wlog))
    assert(!bizLogs.contains(Some(log)))

    val backSeq = lastSeq - 2
    val roll = cache.rollback(backSeq)
    val origV1 = cache.get(key1).get
    val key1_s = redis.hget[Long](key1, "s").get
    val origWLogs = redis.zrange[String](Cache.HISTORY_WRITE_LOG_PRE + "1").get
    val origBizLogs  = redis.lrange[String](Cache.LOG_Q_PREFIX + Cache.mod(key1), 0, -1).get
    val seq = redis.get[Long](Cache.QUORUM_SEQUENCER).get

    assert(roll)
    assert(origV1 == 300)
    assert(key1_s == backSeq)
    assert(!origWLogs.contains(wlog))
    assert(!origBizLogs.contains(Some(log)))
    assert(seq == backSeq)
  }

  it should "count only one even repeat exec" in {
    val wlogId = opIdPre + opId.incrementAndGet
    val key1 = pre + id.incrementAndGet
    val op = Count.decrBy(key1, 10)
    val time = System.currentTimeMillis
    val result = cache.count(wlogId, op, mkLog = true, time, extraLog)
    val effect = result.asInstanceOf[CountSuccessEffect]
    val existWriteLog = cache.hasExecuted(wlogId, op, time, extraLog).get

    val v1 = cache.get(key1).get
    val countLog = cache.popLog(op).get
    val delBackupLog = cache.deleteBackupLog(countLog).get

    assert(existWriteLog.head == 290)
    assert(v1 == 290)
    assert(effect.crs.length == 1)

    assert(effect.crs(0).key == key1)
    assert(effect.crs(0).value == 290)
    assert(effect.crs(0).argValue == -10)
    assert(effect.crs(0).expireScnd == expireScnd)
    assert(delBackupLog == 1)

    assert(countLog.clis.length      == op.length)
    assert(countLog.clis(0).key      == op.head.key)
    assert(countLog.clis(0).value    == effect.crs(0).value)
    assert(countLog.clis(0).argValue == effect.crs(0).argValue)
    assert(countLog.extraLogInfo     == extraLog)

    //repeat
    val r = cache.count(wlogId, op, mkLog = true, time, extraLog).asInstanceOf[CountRepeatEffect]
    val none = cache.popLog(op)
    assert(r.crs(0).key == key1)
    assert(r.crs(0).value == 290)
    assert(none.isEmpty)
  }

  it should "effect only one even repeat exec" in {
    val wlogId = opIdPre + opId.incrementAndGet
    val key1 = pre + id.incrementAndGet
    val key2 = pre + id.incrementAndGet
    val lastSeq = 12

    val coss = Array(CountResult(key1, 290, -10, lastSeq, expireScnd), CountResult(key2, 310, 10, lastSeq, expireScnd))
    val timestamp = System.currentTimeMillis
    val ce = new CountSuccessEffect(wlogId, coss, timestamp, mkLog = true, extraLog)
    val e = cache.effect(ce)
    val existWriteLog = cache.hasExecuted(ce).get

    val v1 = cache.get(key1).get
    val v2 = cache.get(key2).get
    val countLog = cache.popLog(ce).get
    val delBackupLog = cache.deleteBackupLog(ce).get

    assert(e)
    assert(existWriteLog.head == 290)
    assert(existWriteLog(1) == 310)
    assert(v1 == 290)
    assert(v2 == 310)
    assert(delBackupLog == 1)

    assert(countLog.clis.length      == coss.length)
    assert(countLog.clis(0).key      == coss(0).key)
    assert(countLog.clis(0).value    == coss(0).value)
    assert(countLog.clis(0).argValue == coss(0).argValue)

    assert(countLog.clis(1).key      == coss(1).key)
    assert(countLog.clis(1).value    == coss(1).value)
    assert(countLog.clis(1).argValue == coss(1).argValue)
    assert(countLog.extraLogInfo     == extraLog)

    //repeat
    val r = cache.effect(ce)
    val none = cache.popLog(ce)
    assert(r)
    assert(none.isEmpty)
  }
}
