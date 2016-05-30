package com.github.oceanc.countedis.core

import _root_.java.util.concurrent._

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type._
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.{CreateMode, KeeperException}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Random

/**
 * @author chengyang
 */
private[core] class FSM(val cfg: ZkConfig, val selfRedisAddress: String, val selfAddress: String, val hook: QuorumHook) {
  private val log = org.log4s.getLogger
  private val FLAG_SYNC_INIT = "sync_init"
  private val FLAG_SYNC_DONE = "sync_done"
  private val FLAG_SYNC_FAIL = "sync_fail"
  private val quorumRoot = "/quorum"
  private val leaderRoot = "/leader"
  private val leaderNode = "/leader/node"
  private val replicaRedisRoot = "/replicaRedis"
  private val syncRoot = "/election/syncdatda"

  @volatile private var leaderAddr: String = null
  @volatile private var waitSyncLatch: CountDownLatch = null
  @volatile private var waitLeaderLatch: CountDownLatch = null
  private val waitingFor = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("countedis-fsm-waitFor-%d").build)

  @volatile private[core] var state: State = Candidate
  private[core] val followers = new ConcurrentHashMap[String, CopyOnWriteArrayList[String]]

  private val zk = CuratorFrameworkFactory.builder
    .connectString(cfg.conn)
    .namespace(cfg.namespace)
    .connectionTimeoutMs(3000)
    .sessionTimeoutMs(cfg.sessionTimeoutMs)
    .retryPolicy(new ExponentialBackoffRetry(cfg.retrySleepMs, cfg.retryTimes))
    .build
  zk.start()
  zk.create().creatingParentContainersIfNeeded()
    .withMode(CreateMode.EPHEMERAL)
    .forPath(quorumRoot + "/"  + selfAddress, selfRedisAddress.getBytes)

  private val watcher = new Watcher

  def init() = becomeCandidate()

  def destroy(): Unit = {state = Discard; waitingFor.shutdownNow(); watcher.close(); zk.close()}

  def getLeaderAddr = leaderAddr

  def followersInfo: String = followers.entrySet().map(en =>
    "redis[" + en.getKey + "]=countedis" + en.getValue.mkString("[", ", ", "]")
  ).mkString("  ")

  def giveUp(): Unit = {
    state = Discard
    log.warn(s"give up itself (redis=$selfRedisAddress addr=$selfAddress) positively, destroy internal state.")
    destroy()
  }

  def discardRedis(redisAddr: String): Unit = try {
    zk.delete().forPath(replicaRedisRoot + "/" + redisAddr)
  } catch {case _: Throwable =>}

  private def becomeCandidate(): Unit = this.synchronized {
    leaderAddr = null
    watcher.unWatch()
    state = Candidate
    followers.clear()
    hook.notLeader()
    zk.checkExists.forPath(leaderNode) match {
      case null =>
        hook.selfSequence() match {
          case Some(selfSeq) =>
            isEligible(selfSeq) match {
              case true  => becomeLeader(selfSeq)
              case false =>
                log.info(s"self (redis=$selfRedisAddress addr=$selfAddress) is not eligible to be leader, so waiting for leader in position")
                waitingFor.execute(new WaitLeaderTask)
            }
          case None => becomeDiscard()
        }
      case _ => becomeSync()
    }
  }

  private def becomeSync(): Unit = this.synchronized {
    state = Sync
    leaderSeq match {
      case None => becomeCandidate()
      case Some(seq) =>
        val seqPath = syncRoot + "/" + seq + "=" + selfRedisAddress
        zk.checkExists().forPath(seqPath) match {
          case null =>
            try {
              zk.create.withMode(CreateMode.EPHEMERAL).forPath(seqPath, FLAG_SYNC_INIT.getBytes)
              // execute rollback
              hook.rollback(seq.toLong) match {
                case true  => zk.setData().forPath(seqPath, FLAG_SYNC_DONE.getBytes); becomeFollower()
                case false => zk.setData().forPath(seqPath, FLAG_SYNC_FAIL.getBytes); becomeDiscard()
              }
            } catch {
              case _: KeeperException.NodeExistsException => waitingFor.execute(new WaitSyncDoneTask)
            }
          case _ =>
            new String(zk.getData.forPath(seqPath)) match {
              case FLAG_SYNC_DONE => becomeFollower()
              case FLAG_SYNC_FAIL => becomeDiscard()
              case _ => waitingFor.execute(new WaitSyncDoneTask)
            }
        }
    }
  }

  private def becomeFollower(): Unit = this.synchronized {
    if (state != Follower) {
      state = Follower
      log.info(s"self (redis=$selfRedisAddress addr=$selfAddress) become follower.")
      // start watcher on leader
      watcher.watchLeader()
      // retrieve leader address and trigger connect event
      leaderAddress match {
        case Some(addr) => leaderAddr = addr; hook.quorumConnect(new ConnectEvent(addr))
        case None => becomeCandidate()
      }
    }
  }

  private def becomeLeader(seq: Long): Unit = this.synchronized {
    try {
      zk.create.withMode(CreateMode.EPHEMERAL).forPath(leaderNode, (selfAddress + "=" + seq).getBytes)
      log.info(s"self (redis=$selfRedisAddress addr=$selfAddress) become leader.")

      // start watcher on followers
      watcher.watchQuorum()

      // retrieve and cache follower addresses for log
      followersAddress.foreach { a =>
        followers.putIfAbsent(a._1, new CopyOnWriteArrayList[String]())
        followers.get(a._1).remove(a._2)
        followers.get(a._1).add(a._2)
      }
      log.info(s"follower info : $followersInfo")

      followers.keys.foreach{redisAddr => try {
          zk.create.withMode(CreateMode.EPHEMERAL).forPath(replicaRedisRoot + "/" + redisAddr)
        } catch {case _: KeeperException.NodeExistsException =>}
      }

      // extract follower address and trigger connect event
      followers.values().foreach(adrs => adrs.foreach(add => hook.quorumConnect(new ConnectEvent(add))))
      hook.beLeader()
      state = Leader
    } catch {
      case _: KeeperException.NodeExistsException => becomeCandidate()
    }
  }

  private def becomeDiscard(): Unit = this.synchronized {
    watcher.unWatch()
    watcher.unWatchSync()
    state = Discard
  }

  private def isEligible(selfSeq: Long): Boolean = {
    val redisFollower = mutable.Map.empty[String, List[String]]
    followersAddress.foreach { a =>
      redisFollower.get(a._1) match {
        case None       => redisFollower += (a._1 -> List(a._2))
        case Some(list) => redisFollower += (a._1 -> (a._2 :: list))
      }
    }

    def askSeq(remotes: List[String]): Option[Long] = {
      var seq: Option[Long] = None
      for {add <- remotes; if seq.isEmpty} seq = hook.quorumSequence(add)
      seq
    }

    val seqs = redisFollower.keys.map(k => askSeq(redisFollower(k)))
    val existCandidate = seqs.exists{
      case Some(l) => l < selfSeq
      case None    => false
    }
    if (!existCandidate) true else false
  }

  private def leaderSeq: Option[String] = try {
    val leaderData = new String(zk.getData.forPath(leaderNode))
    Some(leaderData.split("=")(1))
  } catch {
    case e: Throwable => log.warn(e)("retrieve leader sequence failed."); None
  }

  private def leaderAddress: Option[String] = try {
    val data = new String(zk.getData.forPath(leaderNode))
    Some(data.split("=")(0))
  } catch {
    case e: Throwable => log.warn(e)("retrieve leader address failed."); None
  }

  private def followersAddress: List[(String, String)] = {
    val followersInfo = zk.getChildren.forPath(quorumRoot).toList
    followersInfo.map { rep =>
      val bs = try {
        Some(zk.getData.forPath(quorumRoot + "/" + rep))
      } catch {
        case e: Throwable =>
          log.warn(s"${quorumRoot + "/" + rep} disappear when retrieve follower address.")
          None
      }
      bs match {
        case None    => (selfRedisAddress, "")
        case Some(b) => (new String(b), rep)
      }
    }.filter{_._1 != selfRedisAddress}
  }

  private class WaitLeaderTask extends Runnable {
    override def run(): Unit = {
      waitLeaderLatch = new CountDownLatch(1)
      watcher.watchLeader()
      val time = 150 + Random.nextInt(150)
      waitLeaderLatch.await(time, TimeUnit.MILLISECONDS) match {
        case true  =>
          log.info(s"self (redis=$selfRedisAddress addr=$selfAddress) waiting for leader in position success, so start data sync for become follower.")
          watcher.unWatch()
        case false =>
          log.info(s"self (redis=$selfRedisAddress addr=$selfAddress) has waited ${time}ms for leader in position, it's too long so trigger elect")
          watcher.unWatch()
          becomeCandidate()
      }
    }
  }

  private class WaitSyncDoneTask extends Runnable {
    override def run(): Unit = {
      waitSyncLatch = new CountDownLatch(1)
      watcher.watchSync()
      val time = 150 + Random.nextInt(150)
      waitSyncLatch.await(time, TimeUnit.MILLISECONDS) match {
        case true  =>
          log.info(s"self (redis=$selfRedisAddress addr=$selfAddress) waiting data sync for become follower success")
          watcher.unWatchSync()
        case false =>
          log.info(s"self (redis=$selfRedisAddress addr=$selfAddress) has waited ${time}ms for data sync to become follower, it's too long so trigger elect")
          watcher.unWatchSync()
          becomeCandidate()
      }
    }
  }

  private class Watcher() {
    private val leaderWatcher = new PathChildrenCache(zk, leaderRoot, true)
    private val leaderListener = new PathChildrenCacheListener {
      override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent) = {
        event.getType match {
          case CHILD_REMOVED if event.getData.getPath == leaderNode =>
            val addr = new String(event.getData.getData)
            log.info(s".........leader node $addr removed, trigger next election")
            becomeCandidate()
          case CHILD_ADDED  if event.getData.getPath == leaderNode && state == Candidate =>
            log.info(s"new leader ${new String(event.getData.getData)} in position")
            if (waitLeaderLatch != null && waitLeaderLatch.getCount > 0) waitLeaderLatch.countDown()
            becomeSync()
          case _ =>
        }
      }
    }

    private val quorumWatcher = new PathChildrenCache(zk, quorumRoot, true)
    private val quorumListener = new PathChildrenCacheListener {
      override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent) = {
        event.getType match {
          case CHILD_ADDED =>
            val countedisAddress = event.getData.getPath.drop(event.getData.getPath.lastIndexOf("/") + 1)
            val followerRedisAddress = new String(event.getData.getData)
            if (selfRedisAddress != followerRedisAddress) {
              followers.putIfAbsent(followerRedisAddress, new CopyOnWriteArrayList[String]())
              followers.get(followerRedisAddress).remove(countedisAddress)
              followers.get(followerRedisAddress).add(countedisAddress)
              log.info(s"new follower joined, redis address=$followerRedisAddress, countedis address=$countedisAddress")
              log.info(s"follower info : $followersInfo")
            }
            hook.quorumConnect(new ConnectEvent(countedisAddress))
          case CHILD_REMOVED =>
            val countedisAddress = event.getData.getPath.drop(event.getData.getPath.lastIndexOf("/") + 1)
            val redisAddress = new String(event.getData.getData)
            val rs = followers.get(redisAddress)
            if (rs != null) rs.remove(countedisAddress)
            followers.synchronized {
              if (rs != null && rs.size == 0) followers.remove(redisAddress)
            }
            log.info(s"one follower leave us, redis address=$redisAddress, countedis address=$countedisAddress")
            log.info(s"follower info : $followersInfo")
            hook.quorumClose(new CloseEvent(countedisAddress))
          case _ =>
        }
      }
    }

    private val replicaRedisWatcher = new PathChildrenCache(zk, replicaRedisRoot, true)
    private val replicaRedisListener = new PathChildrenCacheListener {
      override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent) = event.getType match {
        case CHILD_REMOVED if state == Follower =>
          zk.checkExists().forPath(leaderNode) match {
            case null =>
            case _    =>
              val redisAddr = event.getData.getPath.replace(replicaRedisRoot + "/", "")
              log.info(s"redisAddr=$redisAddr has removed.")
              if (selfRedisAddress == redisAddr) {
                log.info(s"selfRedisAddress $redisAddr has removed. so discard self.")
                giveUp()
              }
          }
      }
    }

    private val syncDataWatcher = new PathChildrenCache(zk, syncRoot, false)
    private val syncDataListener = new PathChildrenCacheListener {
      override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent) = event.getType match {
        case CHILD_UPDATED if state == Sync =>
          val seqRedis = event.getData.getPath.drop(event.getData.getPath.lastIndexOf("/") + 1)
          val data = new String(client.getData.forPath(event.getData.getPath))
          log.info(s"node $seqRedis sync data finished, the state update to $data")
          if (waitSyncLatch != null && waitSyncLatch.getCount > 0) waitSyncLatch.countDown()
          becomeSync()
        case _ =>
      }
    }

    leaderWatcher.start(StartMode.BUILD_INITIAL_CACHE)
    quorumWatcher.start(StartMode.BUILD_INITIAL_CACHE)
    replicaRedisWatcher.start(StartMode.BUILD_INITIAL_CACHE)
    syncDataWatcher.start(StartMode.BUILD_INITIAL_CACHE)

    def unWatch(): Unit = this.synchronized {
      quorumWatcher.getListenable.removeListener(quorumListener)
      leaderWatcher.getListenable.removeListener(leaderListener)
      replicaRedisWatcher.getListenable.removeListener(replicaRedisListener)
    }

    def watchLeader(): Unit = this.synchronized {
      quorumWatcher.getListenable.removeListener(quorumListener)
      leaderWatcher.getListenable.addListener(leaderListener)
      replicaRedisWatcher.getListenable.addListener(replicaRedisListener)
    }

    def watchQuorum(): Unit = this.synchronized {
      leaderWatcher.getListenable.removeListener(leaderListener)
      quorumWatcher.getListenable.addListener(quorumListener)
    }

    def watchSync(): Unit = syncDataWatcher.getListenable.addListener(syncDataListener)

    def unWatchSync() : Unit = syncDataWatcher.getListenable.removeListener(syncDataListener)

    def close(): Unit = {
      unWatch()
      unWatchSync()
      quorumWatcher.close()
      leaderWatcher.close()
      syncDataWatcher.close()
    }
  }
}

case class ZkConfig(conn: String, namespace: String, sessionTimeoutMs: Int, retrySleepMs: Int, retryTimes: Int)

sealed trait State
case object Candidate extends State
case object Sync      extends State
case object Leader    extends State
case object Follower  extends State
case object Discard   extends State

case class ConnectEvent(address: String)
case class CloseEvent(address: String)

trait QuorumHook {
  private[core] def beLeader(): Unit
  private[core] def notLeader(): Unit
  private[core] def quorumConnect(evt: ConnectEvent): Unit
  private[core] def quorumClose(evt: CloseEvent): Unit
  private[core] def quorumSequence(quorumAddr: String): Option[Long]
  private[core] def selfSequence(): Option[Long]
  private[core] def rollback(correctSeq: Long): Boolean
}