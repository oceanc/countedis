package com.github.oceanc.countedis.core

import _root_.java.net.InetAddress
import _root_.java.util.concurrent.atomic.AtomicInteger

import com.github.oceanc.countedis.core.CountedisFactory.CountedisCfg
import com.github.oceanc.countedis.net._
import io.netty.channel.Channel

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.forkjoin.ForkJoinPool

/**
 * @author chengyang
 */
class Countedis(conf: CountedisCfg) {
  private val log = org.log4s.getLogger

  private[core] val op = new Operator(conf)

  def start() = op.start()

  def close() = op.close()

  def isLeader: Boolean = op.fsm.state == Leader

  /**
   * get current follower's information when self state is leader.
   * @return
   */
  def followersInfo: String = op.fsm.followersInfo

  /**
   * get current value.<p>
   * if the instance state is follower and it's cache doesn't exist key,
   * the key/value will retrieve from leader's cache.
   * @param key key
   * @return corresponding value
   */
  def get(key: String): Long = op.cache.get(key) match {
    case Some(l) => l
    case None    => op.loader.load(key)
  }

  /**
   * get current sequence.
   * @return sequence number
   */
  def getCurrentSequence: Long = op.cache.getSequence.get

  /**
   * add 1 to the value of given key.
   * @param key key
   * @return @see [[com.github.oceanc.countedis.core.CountedisResult]]
   */
  def incr(key: String): CountedisResult = incrBy(key, 1)

  /**
   * add 1 to the value of given key and generate consumable log data.
   * @param key key
   * @param extraLogInfo dditional log information for consume.
   * @return @see [[com.github.oceanc.countedis.core.CountedisResult]]
   */
  def incr(key: String, extraLogInfo: String): CountedisResult = incrBy(key, 1, extraLogInfo)

  /**
   * increase specified value of the given key.
   * @param key   key
   * @param count the value of the increasement
   * @return @see [[com.github.oceanc.countedis.core.CountedisResult]]
   */
  def incrBy(key: String, count: Long): CountedisResult = incrBy(key, count, null)

  /**
   * increase specified value of the given key and generate consumable log data.
   * @param key   key
   * @param count the value of the increasement
   * @param extraLogInfo dditional log information for consume.
   * @return @see [[com.github.oceanc.countedis.core.CountedisResult]]
   */
  def incrBy(key: String, count: Long, extraLogInfo: String): CountedisResult = op.exec(Count.incrBy(key, count), extraLogInfo)

  /**
   * reduce 1 to the value of given key
   * @param key key
   * @return @see [[com.github.oceanc.countedis.core.CountedisResult]]
   */
  def decr(key: String): CountedisResult = decrBy(key, 1, null)

  /**
   * reduce 1 to the value of given key and generate consumable log data.
   * @param key  key
   * @param extraLogInfo dditional log information for consume.
   * @return @see [[com.github.oceanc.countedis.core.CountedisResult]]
   */
  def decr(key: String, extraLogInfo: String): CountedisResult = decrBy(key, 1, extraLogInfo)

  /**
   * decrease specified value of the given key.
   * @param key   key
   * @param count the value of the decrement
   * @return @see [[com.github.oceanc.countedis.core.CountedisResult]]
   */
  def decrBy(key: String, count: Long): CountedisResult = decrBy(key, count, null)

  /**
   * decrease specified value of the given key and generate consumable log data.
   * @param key   key
   * @param count the value of the decrement
   * @param extraLogInfo dditional log information for consume.
   * @return @see [[com.github.oceanc.countedis.core.CountedisResult]]
   */
  def decrBy(key: String, count: Long, extraLogInfo: String): CountedisResult = op.exec(Count.decrBy(key, count), extraLogInfo)

  /**
   * atomic method.<p>
   * transfer value from one to another.
   * @param from source key id.
   * @param to target key id.
   * @param count value to transfer.
   * @return @see [[com.github.oceanc.countedis.core.CountedisResult]]
   */
  def transfer(from: String, to: String, count: Long): CountedisResult = transfer(from, to, count, null)

  /**
   * atomic method.<p>
   * transfer value from one to another and generate consumable log data.
   * @param from source key id.
   * @param to target key id.
   * @param count value to transfer.
   * @param extraLogInfo additional log information for consume.
   * @return @see [[com.github.oceanc.countedis.core.CountedisResult]]
   */
  def transfer(from: String, to: String, count: Long, extraLogInfo: String): CountedisResult = op.exec(Count.transfer(from, to, count), extraLogInfo)

}

/**
 * Countedis operation result.
 * @param state
 *              <p>-1 means countedis exclude from cluster, you should not use it again.<p>
 *              0 means failure.<p>
 *              1 means success.<p>
 *              2 means failure cause insufficient.<p>
 *              3 means failure cause overflow.
 * @param kv    java.util.Map consist of (key -> operate result).<p>
 *              if the state is not equal 1(success), the kv is a empty map.
 */
case class CountedisResult(state: Int, kv: java.util.Map[String, Long])

/**
 * Recommend way to build Countedis instance.
 * You could specify coutedis's host ip, the default value is the machine's ip.<p>
 * Also, you could specify coutedis's port for communication between leader and replicas,
 * the port's default value is 7890.
 *
 * @example
 * {{{
 * in java:
 *
 *
 * in scala:
 *
 *  CountedisFactory.builder("127.0.0.1", 7890)
 *  .redisAddress("127.0.0.1:6379")
 *  .zkAddress("127.0.0.1:2181")
 *  .zkSessionTimeoutMs(3000)
 *  .cacheLoader(new CacheLoader {override def load(key: String): Long = ...})
 *  .logConsumer(new LogConsumer {override def consume(log: CountLog): Unit = ...}, 5)
 *  .expireSecond(36000) // 10 hours
 *  .build()
 *
 *  or if you do not need the log data, you can disable log,
 *  and you DON'T need to provide the LogConsumer instance:
 *
 *  CountedisFactory.builder(7891)
 *  .redisAddress("127.0.0.1:6380")
 *  .zkAddress("127.0.0.1:2181")
 *  .cacheLoader(new CacheLoader {override def load(key: String): Long = ...})
 *  .disableLog()
 *  .expireSecond(36000) // 10 hours
 *  .build()
 * }}}
 */
object CountedisFactory {
  private val DEFAULT_PORT = 7890

  def builder()                         = new CountedisCfg(InetAddress.getLocalHost.getHostAddress, DEFAULT_PORT)
  def builder(host: String)             = new CountedisCfg(host, DEFAULT_PORT)
  def builder(port: Int)                = new CountedisCfg(InetAddress.getLocalHost.getHostAddress, port)
  def builder(host: String, port: Int)  = new CountedisCfg(host, port)

  class CountedisCfg(
                     private[core] val ip: String,
                     private[core] val port: Int,
                     private[core] val redisHost: String = null,
                     private[core] val redisPort: Int = 0,
                     private[core] val expireScnd: Int = 0,
                     private[core] val loader: CacheLoader = null,
                     private[core] val zkConn: String = null,
                     private[core] val zkNs: String = "countedis",
                     private[core] val zkSTimeout: Int = 3000,
                     private[core] val zkRetrySleepMs: Int = 300,
                     private[core] val zkRetryTimes: Int = 3,
                     private[core] val mkLog: Boolean = true,
                     private[core] val csmr: LogConsumer = null,
                     private[core] val csmrThread: Int = 1
                           ) {
    def zkAddress(address: String) = new CountedisCfg(ip, port, redisHost, redisPort, expireScnd, loader, address,
      zkNs, zkSTimeout, zkRetrySleepMs, zkRetryTimes, mkLog, csmr, csmrThread)

    def zkSessionTimeoutMs(timeoutMillis: Int) = new CountedisCfg(ip, port, redisHost, redisPort, expireScnd, loader, zkConn,
      zkNs, timeoutMillis, zkRetrySleepMs, zkRetryTimes, mkLog, csmr, csmrThread)

    def zkNamespace(ns: String) = new CountedisCfg(ip, port, redisHost, redisPort, expireScnd, loader, zkConn,
      ns, zkSTimeout, zkRetrySleepMs, zkRetryTimes, mkLog, csmr, csmrThread)

    def redisAddress(address: String) = {
      val hp = address.split(":")
      assert(hp.length == 2)
      new CountedisCfg(ip, port, hp(0), hp(1).toInt, expireScnd, loader, zkConn,
        zkNs, zkSTimeout, zkRetrySleepMs, zkRetryTimes, mkLog, csmr, csmrThread)
    }

    def expireSecond(n: Int) = {
      assert(n > 0)
      new CountedisCfg(ip, port, redisHost, redisPort, n, loader, zkConn,
        zkNs, zkSTimeout, zkRetrySleepMs, zkRetryTimes, mkLog, csmr, csmrThread)
    }

    def cacheLoader(ldr: CacheLoader) = new CountedisCfg(ip, port, redisHost, redisPort, expireScnd, ldr, zkConn,
      zkNs, zkSTimeout, zkRetrySleepMs, zkRetryTimes, mkLog, csmr, csmrThread)

    def disableLog() = new CountedisCfg(ip, port, redisHost, redisPort, expireScnd, loader, zkConn,
      zkNs, zkSTimeout, zkRetrySleepMs, zkRetryTimes, mkLog = false, csmr, csmrThread)

    def logConsumer(consumer: LogConsumer, threadNum: Int) = new CountedisCfg(ip, port, redisHost, redisPort, expireScnd, loader, zkConn,
      zkNs, zkSTimeout, zkRetrySleepMs, zkRetryTimes, mkLog, consumer, threadNum)

    def build() = {
      assert(ip != null)
      assert(port > 0 )
      assert(redisHost !=  null)
      assert(redisPort > 0)
      assert(expireScnd >= 0)
      assert(loader != null)
      assert(zkConn !=  null)
      if (mkLog) assert(csmr != null)
      new Countedis(this)
    }
  }
}

class Operator(conf: CountedisCfg) extends QuorumHook {
  import InternalUtil._
  private val log = org.log4s.getLogger

  private val DISCARD      = -1
  private val FAILURE      = 0
  private val SUCCESS      = 1
  private val INSUFFICIENT = 2
  private val OVERFLOW     = 3

  private val opId = new AtomicInteger(0)
  private val opIdPre = (conf.ip + conf.port).replaceAll("\\.", "")
  private val maxRetry = 3

  private val mkLog = conf.mkLog
  private val selfAddress = conf.ip + ":" + conf.port
  private val selfRedisAddress = conf.redisHost + ":" + conf.redisPort
  private val zkCfg = ZkConfig(conf.zkConn, conf.zkNs, conf.zkSTimeout, conf.zkRetrySleepMs, conf.zkRetryTimes)
  private val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(5))//Runtime.getRuntime.availableProcessors))

  private val msgCodec = new MessageCodec
  private val node = new NioNode {
    override val localPort = conf.port
    override val attrs = Map.empty[String, Countedis]
    override val servCodec: MessageCodec = msgCodec
    override val cliCodec: MessageCodec = msgCodec
  }

  node.start()
  log.info("Countedis instance start servicing......")

  private[core] val fsm = new FSM(zkCfg, selfRedisAddress, selfAddress, this)
  private[core] lazy val loader = new DefaultCacheLoader(kvCode, node, fsm, conf.loader)
  private[core] lazy val cache = new Cache(conf.redisHost, conf.redisPort, loader, conf.expireScnd)
  private[core] lazy val logEvt = new LogEvtContainer(conf.csmr, cache, node, conf.csmrThread)

  msgCodec.bind(Codec.spawn[WriteReply](repCode, Codec.Kryo, LgScala))
  msgCodec.bind(Codec.spawn[ReadSequence](seqCode, Codec.Kryo, LgScala))
  msgCodec.bind(Codec.spawn[CountSuccessEffect](ceCode, Codec.Kryo, LgScala))
  msgCodec.bind(Codec.spawn[WriteCount](ctCode, Codec.Kryo, LgScala))
  msgCodec.bind(Codec.spawn[ReadKV](kvCode, Codec.Kryo, LgScala))
  msgCodec.bind(Codec.spawn[CountLog](dlCode, Codec.Kryo, LgScala))

  node.bindServ(seqCode, new SequenceReader(cache))
  node.bindServ(ceCode,  new FollowerWriter(cache))
  node.bindServ(dlCode,  new FollowerWriter(cache))
  node.bindServ(ctCode,  new LeaderWriter(this, mkLog))
  node.bindServ(kvCode,  new LeaderReader(cache, conf.loader))


  def start() = {
    cache.init()
    fsm.init()
    log.info(s"self (redis=$selfRedisAddress addr=$selfAddress) has joined in the cluster ......")
  }

  def close() = {
    fsm.destroy()
    node.close()
    cache.close()
  }

  def exec(op: List[Count], logInfo: String): CountedisResult = {
    val id = opIdPre + opId.incrementAndGet
    val timestamp = System.currentTimeMillis
    fsm.state match {
      case Leader =>
        cache.count(id, op, mkLog, timestamp, logInfo) match {
          case ce: CountSuccessEffect =>
            copyToFollowers(ce)
            logEvt.add(Cache.mod(ce.crs(0).key))
            CountedisResult(SUCCESS, ce.crs.map(c => c.key -> c.value).toMap.asJava)
          case cr: CountRepeatEffect  =>
            CountedisResult(SUCCESS, cr.crs.map(c => c.key -> c.value).toMap.asJava)
          case fail: CountFailEffect  =>
            if (fail.insufficient)  CountedisResult(INSUFFICIENT, Map.empty[String ,Long].asJava)
            else if (fail.overflow) CountedisResult(OVERFLOW, Map.empty[String ,Long].asJava)
            else { giveUp();        CountedisResult(DISCARD, Map.empty[String ,Long].asJava) }
        }
      case Follower => requestToLeader(id, op, timestamp, logInfo, 0)
      case Discard  => CountedisResult(DISCARD, Map.empty[String ,Long].asJava)
      case _        => CountedisResult(FAILURE, Map.empty[String ,Long].asJava)
    }
  }

  private def requestToLeader(id: String, op: List[Count], timestamp: Long, logInfo: String, times: Int): CountedisResult = {
    def go(n: Int): CountedisResult = {
      val t = n + 1
      val f = node.ask[WriteReply](fsm.getLeaderAddr, Message(ctCode, WriteCount(id, op, logInfo, timestamp)))(900)
      try {
        val wr = Await.result[WriteReply](f, 950.millis)
        wr.success match {
          case true  => CountedisResult(SUCCESS, wr.kv.get.asJava)
          case false => t > maxRetry match {
            case true  => CountedisResult(FAILURE, Map.empty[String ,Long].asJava)
            case false => go(t)
          }
        }
      } catch {case e: Throwable =>
        log.warn(e)("request to leader failed.")
        cache.hasExecuted(id, op, timestamp, logInfo) match {
          case Some(l) => CountedisResult(SUCCESS, op.indices.map(i => op(i).key -> l(i)).toMap.asJava)
          case None    => t > maxRetry match {
            case true  => CountedisResult(FAILURE, Map.empty[String ,Long].asJava)
            case false => go(t)
          }
        }
      }
    }

    go(0)
  }

  private[core] def copyToFollowers(ce: CountSuccessEffect): Unit = {
    val proxies = fsm.followers.entrySet.toList.filter(_.getValue.size > 0).par
    proxies.tasksupport = taskSupport
    proxies.foreach {aa =>
      val redisAddr = aa.getKey
      if (!InternalUtil.tryCopyToRedis(ceCode, ce, node, redisAddr, aa.getValue.toList, maxRetry)) {
        log.warn(s"discard redis=$redisAddr and close it's proxy connection cause try to copy $ce 3 times but failed")
        fsm.discardRedis(redisAddr)
        val rs = fsm.followers.remove(redisAddr)
        try {rs.toList.foreach(node.closeClient)} catch {case _: Throwable =>}
      }
    }
  }

  private[core] def giveUp(): Unit = try { close() } catch { case e: Throwable => log.info(e)("close self.") }

  override private[core] def beLeader(): Unit = logEvt.run(fsm.followers.entrySet.toList.filter(_.getValue.size > 0).par)

  override private[core] def notLeader(): Unit = logEvt.stop()

  override private[core] def quorumConnect(evt: ConnectEvent): Unit = {
    val hp = evt.address.split(":")
    node.connect(hp(0), hp(1).toInt)
    log.info(s"build connection to remote[${evt.address}] success.")
  }

  override private[core] def quorumClose(evt: CloseEvent): Unit = {
    node.closeClient(evt.address)
    log.info(s"close connection of remote[${evt.address}] success.")
  }

  override private[core] def quorumSequence(quorumAddr: String): Option[Long] = {
    def go(n: Int, channel: Channel): Option[Long] = {
      val t = n + 1
      val f = node.ask[ReadSequence](quorumAddr, Message(seqCode, ReadSequence(None)), channel)(900)
      try {
        val cs = Await.result[ReadSequence](f, 950.millis)
        cs.value match {
          case Some(l) => cs.value
          case None    => if(t < maxRetry) go(t, channel) else {log.warn(s"------get $quorumAddr sequence failed."); None}
        }
      } catch {case e: Throwable =>
        log.warn(e)(s"get $quorumAddr sequence failed.")
        if(t < maxRetry) go(t, channel) else None
      }
    }

    var channel: Channel = null
    val hp = quorumAddr.split(":")
    try {
      channel = node.connect(hp(0), hp(1).toInt, ephemeral = true)
      go(0, channel)
    } catch {
      case e: Throwable => log.warn(e)(s"++++++get $quorumAddr sequence failed."); None
    } finally {
      if(channel != null) channel.close()
    }
  }

  override private[core] def selfSequence(): Option[Long] = cache.getSequence

  override private[core] def rollback(correctSeq: Long): Boolean = try {
    cache.rollback(correctSeq)
  } catch { case e: Throwable => log.info(e)("rollback failed."); false }

}