package com.github.oceanc.countedis.core

import com.github.oceanc.countedis.net.{Message, NioClient}
import com.redis.RedisClientPool
import com.redis.serialization.Format.default
import com.redis.serialization.Parse.Implicits._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.Source

/**
 * @author chengyang
 */
class Cache(host: String, port: Int, val loader: CacheLoader, val expireScnd: Int = 0) {
  import Cache._

  private val log = org.log4s.getLogger
//  private val redis = new RedisClient(host, port, database = 0, timeout = 5000)

  private val clients = new RedisClientPool(host, port)

  private val funInit     = clients.withClient(_.scriptLoad(SCRIPT_INIT).get)
  private val funLoad     = clients.withClient(_.scriptLoad(SCRIPT_LOAD).get)
  private val funCount    = clients.withClient(_.scriptLoad(SCRIPT_COUNT).get)
  private val funEffect   = clients.withClient(_.scriptLoad(SCRIPT_EFFECT).get)
  private val funExist    = clients.withClient(_.scriptLoad(SCRIPT_EXIST).get)
  private val funRollback = clients.withClient(_.scriptLoad(SCRIPT_ROLLBACK).get)

  def init(seqNum: Long = 0): Boolean = {
    val args = if (seqNum == 0) List.empty[Any] else List(seqNum)
    clients.withClient(_.evalSHA[Array[Byte]](funInit, args, List.empty[Any]) match {
      case None     => false
      case Some(bs) => new String(bs) match {
        case OK           => true
        case INVALID_ARGS => log.error("init failed. cause lua arguments is invalid. please check your code."); false
        case other        => log.error(s"init ailed. redis return message is $other"); false
      }
    })
  }

  def close(): Unit = clients.withClient(_.quit)

  def get(key: String): Option[Long] = clients.withClient(_.hget[Long](key, "v"))

  def getSequence: Option[Long] = clients.withClient(_.get[Long](QUORUM_SEQUENCER))

  def count(id: String, op: List[Count], mkLog: Boolean, timestamp: Long, extraLogInfo: String): CountEffect = {
    val keys = op.map(_.key)
    var argv = op.flatMap(c => List(
      c.value,
      if (c.lowBound == DEFAULT_LOW_BOUND) _US else c.lowBound,
      if (c.upBound  == DEFAULT_UP_BOUND)  _US else c.upBound,
      if (expireScnd == 0)                 _US else expireScnd
    )) ++ Seq[Any](id, timestamp)
    if (mkLog) {
      argv = argv ++ Seq[Any](mod(keys.head), if (extraLogInfo == null) _US else extraLogInfo)
    }

    try {
      clients.withClient(_.evalSHA[Array[Byte]](funCount, keys, argv) match {
        case None => new CountFailEffect(sysErr = true)
        case Some(bs) => new String(bs) match {
          case xx if xx.startsWith(OK)           =>
            val rvs = xx.split(AT)(1).split(";").map(vs => vs.split(",").map(_.toLong))
            val crs = keys.indices.map(i => new CountResult(keys(i), rvs(i)(0), argv(i * 4).toString.toLong, rvs(i)(1), expireScnd))
            new CountSuccessEffect(id, crs.toArray, timestamp, mkLog, extraLogInfo)
          case xx if xx.startsWith(REPEAT_EXEC)  =>
            val vs = xx.split(AT)(1).split(";").map(_.toLong)
            val crs = keys.indices.map(i => new CountResult(keys(i), vs(i), argv(i * 4).toString.toLong, 0, expireScnd))
            new CountRepeatEffect(crs.toArray)
          case xx if xx.startsWith(NO_SUCH_KEY)  =>
            log.debug(s"exec $op failed cause $xx, so trigger loading.")
            this.load(xx.split("=")(1)) match {
              case true  => count(id, op, mkLog, timestamp, extraLogInfo)
              case false => log.error(s"$op triggered loading failed."); new CountFailEffect(sysErr = true)
            }
          case xx if xx.startsWith(INSUFFICIENT) => log.warn(s"exec $op failed cause $xx."); new CountFailEffect(insufficient = true)
          case xx if xx.startsWith(OVERFLOW)     => log.warn(s"exec $op failed cause $xx."); new CountFailEffect(overflow = true)
          case xx if xx.startsWith(INVALID_ARGS) => log.warn(s"exec $op failed cause script arguments is invalid."); new CountFailEffect(sysErr = true)
          case other                             => log.warn(s"exec $op failed cause $other."); new CountFailEffect(sysErr = true)
        }
      })
    } catch { case e: Throwable => log.info(e)("count failed."); new CountFailEffect(sysErr = true) }
  }

  def effect(ce: CountSuccessEffect): Boolean = {
    val keys = ce.crs.map(_.key).toList
    var argv = ce.crs.flatMap(e => List(
      e.value, e.argValue, e.sequence, if (e.expireScnd == 0) _US else e.expireScnd
    )) ++ Seq[Any](ce.id, ce.timestamp)
    if (ce.mkLog) argv = argv ++ Seq[Any](mod(ce.crs.head.key), if (ce.extraLogInfo == null) _US else ce.extraLogInfo)

    clients.withClient(_.evalSHA[Array[Byte]](funEffect, keys, argv.toList) match {
      case None     => log.error(s"effect $ce failed."); false
      case Some(bs) => new String(bs) match {
        case REPEAT_EXEC => true
        case res         =>
          res.startsWith(OK) match {
            case true  => if (res.length > 2) log.info(s"${res.split(AT)(1).split(";")(1)} effect execution is ignored."); true
            case false => log.error(s"effect $ce failed. redis return message is $res"); false
          }
      }
    })
  }

  def load(key: String): Boolean = {
    val value = loader.load(key)
    clients.withClient(_.evalSHA[Array[Byte]](funLoad, List(key), List(value, expireScnd)) match {
      case None => log.error(s"load $key failed."); false
      case Some(res) => new String(res) match {
        case OK => true
        case INVALID_ARGS => log.error(s"load $key failed. cause lua arguments is invalid. please check your code."); false
        case other => log.error(s"load $key failed. redis return message is $other"); false
      }
    })
  }

  def rollback(quorumSequence: Long): Boolean =
    clients.withClient(_.evalSHA[Array[Byte]](funRollback, List(quorumSequence), List.empty[Any]) match {
      case None => log.error(s"rollback to quorum sequence $quorumSequence failed."); false
      case Some(bs) => new String(bs) match {
        case OK => true
        case ROLLBACK_INSUFFICIENT =>
          log.error(s"rollback to quorum sequence $quorumSequence failed cause by not enough rolling log.")
          false
        case other =>
          log.error(s"rollback to quorum sequence $quorumSequence failed cause by $other")
          false
      }
    })

  def popLog(op: List[Count]): Option[CountLog] = rpopLpush(mod(op.head.key))

  def popLog(ce: CountSuccessEffect): Option[CountLog] = rpopLpush(mod(ce.crs.head.key))

  def popLog(from: String): Option[CountLog] = rpopLpush(mod(from))

  def popLog(idx: Int): Option[CountLog] = rpopLpush(idx)

  def deleteLog(ce: CountSuccessEffect) = clients.withClient(_.lrem(LOG_Q_PREFIX + mod(ce.crs.head.key), 1, ce.convertToLog.toLog))

  def deleteLog(cl: CountLog) = clients.withClient(_.lrem(LOG_Q_PREFIX + mod(cl.clis.head.key), 1, cl.toLog))

  def deleteBackupLog(ce: CountSuccessEffect) = clients.withClient(_.lrem(LOG_Q_PREFIX + mod(ce.crs.head.key) + LOG_Q_PROCESSING, 1, ce.convertToLog.toLog))

  def deleteBackupLog(cl: CountLog) = clients.withClient(_.lrem(LOG_Q_PREFIX + mod(cl.clis.head.key) + LOG_Q_PROCESSING, 1, cl.toLog))

  def countLog: Map[Int, Int] = {
    val logs = (0 until LOG_Q_SHARDING_SIZE).map(_ -> 0).toMap
    logs.map(a =>
      clients.withClient(_.llen(LOG_Q_PREFIX + a._1) match {
        case None    => a._1 -> 0
        case Some(n) => a._1 -> n.toInt
      })
    )
  }

  def hasExecuted(id: String, op: List[Count], timestamp: Long, extraLogInfo: String): Option[List[Long]] = {
    checkWriteLog(Count.convertToWriteLog(id, op, timestamp, extraLogInfo))
  }

  def hasExecuted(ce: CountSuccessEffect): Option[List[Long]] = {
    checkWriteLog(ce.convertToWriteLog)
  }

  private def checkWriteLog(wlog: String): Option[List[Long]] =
    clients.withClient(_.evalSHA[Array[Byte]](funExist, List(wlog), List.empty[Any]) match {
      case None     => log.error(s"check exist of $wlog failed."); None
      case Some(bs) => new String(bs) match {
        case x if x.startsWith("1") => Some(x.split(AT)(1).split(";").map(_.toLong).toList)
        case "0"                    => None
        case INVALID_ARGS           => log.error(s"check exist of $wlog failed. cause lua arguments is invalid. please check your code."); None
        case other                  => log.error(s"check exist of $wlog failed. redis return message is $other"); None
      }
    })

  private def rpopLpush(idx: Int): Option[CountLog] = {
    val from = LOG_Q_PREFIX + idx
    val to   = LOG_Q_PREFIX + idx + LOG_Q_PROCESSING
    clients.withClient(_.rpoplpush[String](from, to) match {
      case None         => None
      case Some(logStr) => Some(CountLog(logStr))
    })
  }

}

object Cache {
  private[core] def mod(key: String): Int = {
    val xx = key.partition(_.isDigit)
    val ss = if (xx._1.length > 15) xx._1.substring(xx._1.length - 15, xx._1.length).toLong else xx._1.toLong
    val nan = LOG_Q_SHARDING_SIZE + xx._2.map(_.toInt).sum
    ((ss + nan).abs % LOG_Q_SHARDING_SIZE).toInt
  }

  private[core] val QUORUM_SEQUENCER      = "countedis.system.quorum.sequencer"
  private[core] val HISTORY_WRITE_LOG_PRE = "countedis.system.history.write.log."
  private[core] val HISTORY_VALUE_LOG_PRE = "countedis.system.history.value.log."
  private val HISTORY_WRITE_LOG_SHARD = 100
  private val HISTORY_PER_LOG_LIMIT = 1000

  private[core] val LOG_Q_PREFIX = "countedis.user.log."
  private[core] val LOG_Q_PROCESSING = ".processing"
  private val LOG_Q_SHARDING_SIZE = 97

  private val DEFAULT_LOW_BOUND = 0
  private val DEFAULT_UP_BOUND = Long.MaxValue //9223372036854775807

  private val _US = "_"
  private val OK = "ok"
  private[core] val AT = "@"
  private[core] val EXTRA_LOG_DELIMITER = "~~~~~~"
  private val INVALID_ARGS = "invalid args length"
  private val NO_SUCH_KEY = "no such key="
  private val INSUFFICIENT = "insufficient="
  private val OVERFLOW = "overflow="
  private val REPEAT_EXEC = "repeat exec"

  private val ROLLBACK_INSUFFICIENT = "rollback insufficient"

  private def loadScript(fileName: String): String = {
    val f = Source.fromInputStream(Cache.getClass.getClassLoader.getResourceAsStream(fileName))
    val s = f.mkString
    f.close()
    s
  }

  private val SCRIPT_INIT = s"""local reply    = '$OK'
                              |local argErr    = '$INVALID_ARGS'
                              |local sequencer = '$QUORUM_SEQUENCER'
                              |""".stripMargin + loadScript("init.lua")

  private val SCRIPT_LOAD = s"""local argErr    = '$INVALID_ARGS'
                               |local sequencer = '$QUORUM_SEQUENCER'
                               |local reply     = '$OK'
                               |""".stripMargin + loadScript("load.lua")

  private val SCRIPT_EXIST = s"""local arrErr  = '$INVALID_ARGS'
                               |local writeLog = '$HISTORY_WRITE_LOG_PRE'
                               |local valueLog = '$HISTORY_VALUE_LOG_PRE'
                               |local wShard   =  $HISTORY_WRITE_LOG_SHARD
                               |local at       = '$AT'
                               |""".stripMargin + loadScript("exist.lua")

  private val SCRIPT_COUNT = s"""local argErr      = '$INVALID_ARGS'
                               |local noKey        = '$NO_SUCH_KEY'
                               |local insufficient = '$INSUFFICIENT'
                               |local overflow     = '$OVERFLOW'
                               |local repeatExec   = '$REPEAT_EXEC'
                               |local sequencer    = '$QUORUM_SEQUENCER'
                               |local qPre         = '$LOG_Q_PREFIX'
                               |local writeLog     = '$HISTORY_WRITE_LOG_PRE'
                               |local valueLog     = '$HISTORY_VALUE_LOG_PRE'
                               |local delimiter    = '$EXTRA_LOG_DELIMITER'
                               |local lowBound     =  $DEFAULT_LOW_BOUND
                               |local upBound      =  $DEFAULT_UP_BOUND
                               |local limit        =  $HISTORY_PER_LOG_LIMIT
                               |local wShard       =  $HISTORY_WRITE_LOG_SHARD
                               |local at           = '$AT'
                               |local reply        = '$OK$AT'
                               |""".stripMargin + loadScript("count.lua")

  private val SCRIPT_EFFECT = s"""local argErr    = '$INVALID_ARGS'
                                |local repeatExec = '$REPEAT_EXEC'
                                |local writeLog   = '$HISTORY_WRITE_LOG_PRE'
                                |local valueLog   = '$HISTORY_VALUE_LOG_PRE'
                                |local sequencer  = '$QUORUM_SEQUENCER'
                                |local qPre       = '$LOG_Q_PREFIX'
                                |local delimiter  = '$EXTRA_LOG_DELIMITER'
                                |local limit      =  $HISTORY_PER_LOG_LIMIT
                                |local wShard     =  $HISTORY_WRITE_LOG_SHARD
                                |local at         = '$AT'
                                |local reply      = '$OK'
                                |""".stripMargin + loadScript("effect.lua")

  private val SCRIPT_ROLLBACK = s"""local arrErr              = '$INVALID_ARGS'
                                  |local rollbackInsufficient = '$ROLLBACK_INSUFFICIENT'
                                  |local sequencer            = '$QUORUM_SEQUENCER'
                                  |local qPre                 = '$LOG_Q_PREFIX'
                                  |local delimiter            = '$EXTRA_LOG_DELIMITER'
                                  |local writeLog             = '$HISTORY_WRITE_LOG_PRE'
                                  |local valueLog             = '$HISTORY_VALUE_LOG_PRE'
                                  |local limit                =  $HISTORY_PER_LOG_LIMIT
                                  |local wShard               =  $HISTORY_WRITE_LOG_SHARD
                                  |local qShard               =  $LOG_Q_SHARDING_SIZE
                                  |local at                   = '$AT'
                                  |local reply                = '$OK'
                                  |""".stripMargin + loadScript("rollback.lua")
}

trait CacheLoader {
  def load(key: String): Long
}

private[core] class DefaultCacheLoader(code: Short, client: NioClient, fsm: FSM, loader: CacheLoader) extends CacheLoader {
  private val log = org.log4s.getLogger
  private val maxRetry = 3
  override def load(key: String): Long = {
    //@annotation.tailrec
    def go(n: Int): Long = {
      val t = n + 1
      fsm.state match {
        case Leader   => loader.load(key)
        case Follower => fsm.getLeaderAddr match {
          case null => loader.load(key)
          case addr =>
            val f = client.ask[ReadKV](addr, Message(bizCode = code, content = ReadKV(key, None)))(250)
            val v = try {
              val kv = Await.result[ReadKV](f, 300.millis)
              kv.value match {
                case Some(l) => l
                case None    => if(t < maxRetry) go(t) else {log.warn(s"load $key from leader failed."); loader.load(key)}
              }
            }  catch {case e: Throwable =>
              log.warn(s"load $key from leader failed.")
              if(t < maxRetry) go(t) else loader.load(key)
            }
            v
        }
        case _  => if(t < maxRetry) go(t) else loader.load(key)
      }
    }
    go(0)
  }
}