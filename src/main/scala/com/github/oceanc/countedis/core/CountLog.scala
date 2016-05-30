package com.github.oceanc.countedis.core

import com.github.oceanc.countedis.net.NioClient
import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.collection.parallel.{ForkJoinTaskSupport, ParSeq}
import scala.concurrent.forkjoin.ForkJoinPool

/**
 * @author chengyang
 */
case class CountLog(clis: Array[CountLogItem], timestamp: Long, extraLogInfo: String) {
  def toLog: String = {
    val baseLogInfo = clis.map(_.toLog).mkString(";") + ";" + timestamp
    if (extraLogInfo == null) baseLogInfo else baseLogInfo + Cache.EXTRA_LOG_DELIMITER + extraLogInfo
  }
  override def toString = "CountLog[clis=" + clis.mkString(",") + " timestamp=" + timestamp + " extraLogInfo=" + extraLogInfo + "]"
}

object CountLog {
  def apply(logInfo: String): CountLog = {
    val be = logInfo.split(Cache.EXTRA_LOG_DELIMITER)
    val ops = be(0).split(";")
    val timestamp = ops(ops.length - 1).toLong
    val clis = (0 until ops.length - 1).map{i =>
      val kvs = ops(i).split(":")
      val vv = kvs(1).split(",")
      new CountLogItem(kvs(0), vv(1).toLong, vv(0).toLong)
    }.toArray
    val extraLogInfo = if (be.length == 2) be(1) else null
    new CountLog(clis, timestamp, extraLogInfo)
  }
}

case class CountLogItem(key: String, value: Long, argValue: Long) {
  def toLog: String = key + ":" + argValue + "," + value
}

trait LogConsumer {
  def consume(log: CountLog): Unit
}

class LogEvtContainer(val csmr: LogConsumer, val cache: Cache, val client: NioClient, n: Int) {self=>
  import _root_.java.util.concurrent.{CopyOnWriteArrayList, ExecutorService, Executors, Semaphore}

  import scala.collection.JavaConversions._

  private val log = org.log4s.getLogger
  private[this] val maxRetry = 3
  private[this] val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(5))//Runtime.getRuntime.availableProcessors))
  private[this] val semaphore = new Semaphore(0)
//  private[this] val q = new mutable.Queue[Int]
  private[this] val q = new java.util.concurrent.ConcurrentLinkedQueue[Integer]
  @volatile private[this] var pool: ExecutorService = null
  @volatile private[this] var poison = true

  def run(fs:  ParSeq[java.util.Map.Entry[String, CopyOnWriteArrayList[String]]]): Unit = {
    fs.tasksupport = taskSupport
    poison = false
    pool = Executors.newFixedThreadPool(n, new ThreadFactoryBuilder().setNameFormat("Log-consumer-%d").build)
    cache.countLog.foreach(a => add(List.fill(a._2)(Integer.valueOf(a._1))))
    (0 until n).foreach(_ =>
      pool.execute(new Runnable {
        override def run(): Unit = {
          while(!poison) {
            semaphore.acquire()
            q.poll match {
              case null =>
              case idx  =>
                cache.popLog(idx) match {
                  case None =>
                  case Some(cl) => try {
                    csmr.consume(cl)
                    cache.deleteBackupLog(cl)
                    copyToFollower(cl, fs)
                  } catch {
                    case t: Throwable => self.log.warn(t)("occur some error when consume count log.")
                  }
                }
            }
          }
        }
      })
    )
  }

  def stop(): Unit = {
    poison = true
    if (pool != null && !pool.isShutdown) {
      try {pool.shutdownNow()} catch {case _: InterruptedException =>}
    }
  }

  def add(a: Int): Unit = {
    q.offer(a)
    semaphore.release()
  }

  private def add(as: List[Integer]): Unit = {
    q.addAll(as)
    semaphore.release(as.size)
  }

  private def copyToFollower(cl: CountLog, fs: ParSeq[java.util.Map.Entry[String, CopyOnWriteArrayList[String]]]): Unit = {
    fs.foreach(a => {
      val redisAddr = a.getKey
      val s = System.currentTimeMillis
      InternalUtil.tryCopyToRedis(InternalUtil.dlCode, cl, client, redisAddr, a.getValue.toList, maxRetry) match {
        case true  =>
          log.debug(s"delete $cl from redis=$redisAddr success.")
        case false =>
          log.warn(s"delete $cl from redis=$redisAddr $maxRetry times cost ${System.currentTimeMillis - s} ms, but failed, cause the follower instance not reply on time.")
      }
    })
  }

}