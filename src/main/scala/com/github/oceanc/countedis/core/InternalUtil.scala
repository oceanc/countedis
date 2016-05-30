package com.github.oceanc.countedis.core

import com.github.oceanc.countedis.net.{NioClient, Message, ServerReply}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

/**
 * @author chengyang
 */
object InternalUtil {
  private val log = org.log4s.getLogger
  val repCode = 0.toShort
  val seqCode = 1.toShort
  val ceCode  = 2.toShort
  val ctCode  = 3.toShort
  val kvCode  = 4.toShort
  val dlCode  = 5.toShort

  private implicit val timeoutMs = 1000
  private val awaitMs = (timeoutMs + 50).millis

  def tryCopyToRedis[A](c: Short, o: A, client: NioClient, redisAddr: String, proxies: List[String], maxRetry: Int): Boolean = {
    doCopy(c, o, client, redisAddr, proxies, maxRetry, 0)
  }

  private def doCopy[A](c: Short, o: A, client: NioClient, redisAddr: String, proxies: List[String], maxRetry: Int, times: Int): Boolean = {
    val msg = Message(c, o)
    val localMaxRetry = maxRetry - proxies.size + 1
    val s = System.currentTimeMillis
    log.debug(s"copy operate $msg to follower redis=$redisAddr start.")

    def random(ls: List[String]): Boolean = {
      val addr = ls(Random.nextInt(ls.size))
      try {
        val f = client.ask[WriteReply](addr, msg)
        Await.result[WriteReply](f, awaitMs).success match {
          case true  =>
            log.debug(s"copy operate $msg to follower $addr[redis=$redisAddr] success, cost ${System.currentTimeMillis - s}ms")
            true
          case false =>
            ls.size match {
              case 1 => loop(0, addr)
              case _ => random(ls.filterNot(_ == addr))
            }
        }
      } catch {case e: Throwable =>
        ls.size match {
          case 1 => loop(0, addr)
          case _ => random(ls.filterNot(_ == addr))
        }
      }
    }

    def loop(n: Int, addr: String): Boolean = {
      log.debug(s"copy operate $msg to follower redis=$redisAddr start for ${n + 1} time")
      if (n <= localMaxRetry) {
        try {
          val f = client.ask[WriteReply](addr, msg)
          log.debug(s"copy operate $msg to follower redis=$redisAddr invoke as cost ${System.currentTimeMillis - s}.")
          Await.result[WriteReply](f, awaitMs).success match {
            case true =>
              log.debug(s"copy operate $msg to follower $addr[redis=$redisAddr] success, cost ${System.currentTimeMillis - s}ms")
              true
            case false => loop(n + 1, addr)
          }
        } catch {case e: Throwable => loop(n + 1, addr)}
      } else {
        log.debug(s"copy operate $msg to follower $addr[redis=$redisAddr] eventual failed, cost ${System.currentTimeMillis - s}ms, may be should discard it.")
        false
      }
    }

    random(proxies)
  }

}

class SequenceReader(cache: Cache) extends ServerReply[ReadSequence] {
  private val log = org.log4s.getLogger
  override def handle(msg: Message[_]): Message[ReadSequence] = try {
    Message(bizCode = InternalUtil.seqCode, content = ReadSequence(cache.getSequence), traceId = msg.traceId)
  } catch {
    case e: Throwable => Message(bizCode = msg.bizCode, content = ReadSequence(None), traceId = msg.traceId)
  }
}

class FollowerWriter(cache: Cache) extends ServerReply[WriteReply] {
  private val log = org.log4s.getLogger
  override def handle(msg: Message[_]): Message[WriteReply] = try {
    msg.bizCode match {
      case InternalUtil.ceCode =>
        val ce = msg.content.asInstanceOf[CountSuccessEffect]
        val re = cache.effect(ce)
        Message(bizCode = InternalUtil.repCode, content = WriteReply(re, None), traceId = msg.traceId)
      case InternalUtil.dlCode =>
        val dl = msg.content.asInstanceOf[CountLog]
        val ok = cache.deleteLog(dl) match {
          case None    => false
          case Some(n) => true
        }
        Message(bizCode = InternalUtil.repCode, content = WriteReply(ok, None), traceId = msg.traceId)
    }
  } catch {
    case e: Throwable =>
      log.error(e)(s"something wrong when exec effect ${msg.content}")
      Message(bizCode = InternalUtil.repCode, content = WriteReply(success = false, None), traceId = msg.traceId)
  }
}

class LeaderWriter(op: Operator, mkLog: Boolean) extends ServerReply[WriteReply] {
  override def handle(msg: Message[_]): Message[WriteReply] = {
    val cr = msg.content.asInstanceOf[WriteCount]
    val wr = op.cache.count(cr.id, cr.op, mkLog, cr.timestamp, cr.extraLogInfo) match {
      case ce: CountSuccessEffect  =>
        op.copyToFollowers(ce)
        op.logEvt.add(Cache.mod(ce.crs(0).key))
        WriteReply(success = true, Some(ce.crs.map(c => c.key -> c.value).toMap))
      case cr: CountRepeatEffect   => WriteReply(success = true, Some(cr.crs.map(c => c.key -> c.value).toMap))
      case fail: CountFailEffect   => if (fail.sysErr) op.giveUp(); WriteReply(success = false, None)
    }
    Message(bizCode = InternalUtil.repCode, content = wr, traceId = msg.traceId)
  }
}

class LeaderReader(cache: Cache, loader: CacheLoader) extends ServerReply[ReadKV] {
  override def handle(msg: Message[_]): Message[ReadKV] = {
    val kv = msg.content.asInstanceOf[ReadKV]
    val v = cache.get(kv.key) match {
      case Some(l) => l
      case None    => if (cache.load(kv.key)) cache.get(kv.key).get else loader.load(kv.key)
    }
    Message(bizCode = msg.bizCode, ReadKV(kv.key, Some(v)), traceId = msg.traceId)
  }
}

case class WriteCount(id: String, op: List[Count], extraLogInfo: String, timestamp: Long)
case class WriteReply(success: Boolean, kv: Option[Map[String, Long]])
case class ReadKV(key: String, value: Option[Long])
case class ReadSequence(value: Option[Long])