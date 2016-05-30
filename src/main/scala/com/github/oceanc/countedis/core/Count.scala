package com.github.oceanc.countedis.core

/**
 * Immutable class.<p>
 * Indicate the value for one key to add or subtract,
 * and the boundary that constraint the result.<p>
 * Default lowBound is 0.<p>
 * Default upBound is Long.MaxValue.
 *
 * @author chengyang
 *
 */
case class Count(key: String, value: Long, lowBound: Long, upBound: Long) {
  require(lowBound < upBound)

  def this(key: String, value: Long) = this(key, value, 0, Long.MaxValue)

  def this(key: String, value: Long, lowBound: Long) = this(key, value, lowBound, Long.MaxValue)

  def withLowBound(low: Long): Count =  new Count(key, value, low, upBound)

  def withUpBound(up: Long): Count   =  new Count(key, value, lowBound, up)

  def freeze(size: Long): Count      =  new Count(key, value, lowBound + size, upBound)

  def overdraw(size: Long): Count    =  new Count(key, value, lowBound - size, upBound)

  def transferTo(otherKey: String)   =  List(new Count(key, -value, lowBound, upBound), new Count(otherKey, value))

  def decr                           =  List(if (value > 0) new Count(key, -value) else this)
  def decrWithLowBound(low: Long)    =  List(new Count(key, if (value > 0) -value else value, low))
  def decrWithFreeze(freeze: Long)   =  List((if (value > 0) new Count(key, -value) else this).freeze(freeze))
  def decrWithOverdraw(over: Long)   =  List((if (value > 0) new Count(key, -value) else this).overdraw(over))

  def incr                           =  List(if (value < 0) new Count(key, -value) else this)
  def incrWithUpBound(up: Long)      =  List(new Count(key, if (value < 0) -value else value, up))
}

object Count {
  def incr(key: String)                                        = new Count(key, 1).incr
  def incrWithUpBound(key: String, up: Long)                   = new Count(key, 1).incrWithUpBound(up)
  def incrBy(key: String, value: Long)                         = new Count(key, value).incr
  def incrByWithUpBound(key: String, value: Long, up: Long)    = new Count(key, value).incr

  def decr(key: String)                                        = new Count(key, -1).decr
  def decrWithLowBound(key: String, low: Long)                 = new Count(key, -1).decrWithLowBound(low)
  def decrWithFreeze(key: String, freeze: Long)                = new Count(key, -1).decrWithFreeze(freeze)
  def decrWithOverdraw(key: String, over: Long)                = new Count(key, -1).decrWithOverdraw(over)

  def decrBy(key: String, value: Long)                         = new Count(key, value).decr
  def decrByWithLowBound(key: String, value: Long, low: Long)  = new Count(key, value).decrWithLowBound(low)
  def decrByWithFreeze(key: String, value: Long, freeze: Long) = new Count(key, value).decrWithFreeze(freeze)
  def decrByWithOverdraw(key: String, value: Long, over: Long) = new Count(key, value).decrWithOverdraw(over)

  def transfer(from: String, to: String, value: Long)          = new Count(from, value).transferTo(to)

  def convertToWriteLog(id: String, op: List[Count], timestamp: Long, extraLogInfo: String): String = {
    val baseLogInfo = id + Cache.AT + op.map{c => c.key + ":" + c.value}.mkString(";") + ";" + timestamp
    if (extraLogInfo == null) baseLogInfo else baseLogInfo + Cache.EXTRA_LOG_DELIMITER + extraLogInfo
  }
}

case class CountResult(key: String, value: Long, argValue: Long, sequence: Long, expireScnd: Int) {
  def convertToLog: CountLogItem = new CountLogItem(key, value, argValue)
}

sealed trait CountEffect
case class CountRepeatEffect(crs: Array[CountResult]) extends CountEffect
case class CountFailEffect(
                             sysErr: Boolean = false,
                             insufficient: Boolean = false,
                             overflow: Boolean = false) extends CountEffect
case class CountSuccessEffect(
                                id: String,
                                crs: Array[CountResult],
                                timestamp: Long,
                                mkLog: Boolean,
                                extraLogInfo: String) extends CountEffect {
  def convertToLog: CountLog = new CountLog(crs.map(_.convertToLog), timestamp, extraLogInfo)

  def convertToWriteLog: String = {
    val baseLogInfo = id + Cache.AT + crs.map{c => c.key + ":" + c.argValue}.mkString(";") + ";" + timestamp
    if (extraLogInfo == null) baseLogInfo else baseLogInfo + Cache.EXTRA_LOG_DELIMITER + extraLogInfo
  }

  override def toString = "CountSuccessEffect[id=" + id + " crs=" + crs.mkString(",") + " timestamp=" + timestamp + " mkLog=" + mkLog + " extraLogInfo=" + extraLogInfo + "]"
}