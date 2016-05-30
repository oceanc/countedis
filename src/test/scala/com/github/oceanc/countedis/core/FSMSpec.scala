package com.github.oceanc.countedis.core

import java.util.concurrent.atomic.AtomicInteger

/**
 * @author chengyang
 */
class FSMSpec extends UnitSpec {

  val zkAddress = "127.0.0.1:2181"
  val zkNamespace = "countedis"

  val _1 = 1
  val _2 = 2
  val _3 = 3

  val r_1 = "127.0.0.1:6379"
  val r_2 = "127.0.0.1:6380"
  val r_3 = "127.0.0.1:6381"

  val l_1 = "127.0.0.1:7890"
  val l_2 = "127.0.0.1:7891"
  val l_3 = "127.0.0.1:7892"
  val l_4 = "127.0.0.1:7893"


  "r_1 mount l_1, r_2 mount l_2, r_3 mount l_3" should "l_1 become leader and others are replica" in {
    val h_1 = new QuorumHook {
      override private[core] def beLeader(): Unit = {}
      override private[core] def notLeader(): Unit = {}
      override private[core] def quorumConnect(evt: ConnectEvent): Unit = {}
      override private[core] def quorumClose(evt: CloseEvent): Unit = {}
      override private[core] def selfSequence(): Option[Long] = Some(_1)
      override private[core] def quorumSequence(quorumAddr: String): Option[Long] = None
      override private[core] def rollback(correctSeq: Long): Boolean = true
    }
    val h_2 = new QuorumHook {
      override private[core] def beLeader(): Unit = {}
      override private[core] def notLeader(): Unit = {}
      override private[core] def quorumConnect(evt: ConnectEvent): Unit = {}
      override private[core] def quorumClose(evt: CloseEvent): Unit = {}
      override private[core] def selfSequence(): Option[Long] = Some(_2)
      override private[core] def quorumSequence(quorumAddr: String): Option[Long] = Some(_3)
      override private[core] def rollback(correctSeq: Long): Boolean = true
    }
    val h_3 = new QuorumHook {
      override private[core] def beLeader(): Unit = {}
      override private[core] def notLeader(): Unit = {}
      override private[core] def quorumConnect(evt: ConnectEvent): Unit = {}
      override private[core] def quorumClose(evt: CloseEvent): Unit = {}
      override private[core] def selfSequence(): Option[Long] = Some(_3)
      override private[core] def quorumSequence(quorumAddr: String): Option[Long] = Some(_2)
      override private[core] def rollback(correctSeq: Long): Boolean = true
    }

    val fsm_1 = new FSM(ZkConfig(zkAddress, zkNamespace, 300, 1000, 3), r_1, l_1, h_1)
    val fsm_2 = new FSM(ZkConfig(zkAddress, zkNamespace, 300, 1000, 3), r_2, l_2, h_2)
    val fsm_3 = new FSM(ZkConfig(zkAddress, zkNamespace, 300, 1000, 3), r_3, l_3, h_3)
    fsm_1.init()
    fsm_2.init()
    fsm_3.init()

    assert(fsm_1.state == Leader)
    assert(fsm_2.state == Follower)
    assert(fsm_3.state == Follower)
    assert(fsm_1.followers.size() == 2)
    assert(fsm_1.followers.get(r_2).get(0) == l_2)

    fsm_1.destroy()
    fsm_2.destroy()
    fsm_3.destroy()
  }

  it should "replica l_2 become leader when leader l_1 close" in {
    val h_1 = new QuorumHook {
      override private[core] def beLeader(): Unit = {}
      override private[core] def notLeader(): Unit = {}
      override private[core] def quorumConnect(evt: ConnectEvent): Unit = {}
      override private[core] def quorumClose(evt: CloseEvent): Unit = {}
      override private[core] def selfSequence(): Option[Long] = Some(_1)
      override private[core] def quorumSequence(quorumAddr: String): Option[Long] = None
      override private[core] def rollback(correctSeq: Long): Boolean = true
    }
    val h_2 = new QuorumHook {
      override private[core] def beLeader(): Unit = {}
      override private[core] def notLeader(): Unit = {}
      override private[core] def quorumConnect(evt: ConnectEvent): Unit = {}
      override private[core] def quorumClose(evt: CloseEvent): Unit = {}
      override private[core] def selfSequence(): Option[Long] = Some(_2)
      override private[core] def quorumSequence(quorumAddr: String): Option[Long] = Some(_3)
      override private[core] def rollback(correctSeq: Long): Boolean = true
    }
    val h_3 = new QuorumHook {
      override private[core] def beLeader(): Unit = {}
      override private[core] def notLeader(): Unit = {}
      override private[core] def quorumConnect(evt: ConnectEvent): Unit = {}
      override private[core] def quorumClose(evt: CloseEvent): Unit = {}
      override private[core] def selfSequence(): Option[Long] = Some(_3)
      override private[core] def quorumSequence(quorumAddr: String): Option[Long] = Some(_2)
      override private[core] def rollback(correctSeq: Long): Boolean = true
    }

    val fsm_1 = new FSM(ZkConfig(zkAddress, zkNamespace, 300, 1000, 3), r_1, l_1, h_1)
    val fsm_2 = new FSM(ZkConfig(zkAddress, zkNamespace, 300, 1000, 3), r_2, l_2, h_2)
    val fsm_3 = new FSM(ZkConfig(zkAddress, zkNamespace, 300, 1000, 3), r_3, l_3, h_3)
    fsm_1.init()
    fsm_2.init()
    fsm_3.init()

    fsm_1.destroy()
    Thread.sleep(200)

    assert(fsm_2.state == Leader)
    assert(fsm_2.followers.size == 1)
    assert(fsm_2.followers.get(r_3).get(0) == l_3)
    assert(fsm_3.state == Follower)
    assert(fsm_3.followers.size == 0)

    fsm_2.destroy()
    fsm_3.destroy()
  }

  "r_1 mount l_1 and l_2, r_3 mount l_3" should "l_2 or l_3 become leader when original leader l_1 close" in {
    val sameHandler = new QuorumHook {
      override private[core] def beLeader(): Unit = {}
      override private[core] def notLeader(): Unit = {}
      override private[core] def quorumConnect(evt: ConnectEvent): Unit = {}
      override private[core] def quorumClose(evt: CloseEvent): Unit = {}
      override private[core] def selfSequence(): Option[Long] = Some(_1)
      override private[core] def quorumSequence(quorumAddr: String): Option[Long] = Some(_1)
      override private[core] def rollback(correctSeq: Long): Boolean = true
    }
    val fsm_1 = new FSM(ZkConfig(zkAddress, zkNamespace, 300, 1000, 3), r_1, l_1, sameHandler)
    val fsm_2 = new FSM(ZkConfig(zkAddress, zkNamespace, 300, 1000, 3), r_1, l_2, sameHandler)
    val fsm_3 = new FSM(ZkConfig(zkAddress, zkNamespace, 300, 1000, 3), r_3, l_3, sameHandler)
    fsm_1.init()
    fsm_2.init()
    fsm_3.init()

    fsm_1.destroy()
    Thread.sleep(200)

    if (fsm_2.state == Leader) {
      assert(fsm_2.followers.size == 1)
      assert(fsm_2.followers.get(r_3).get(0) == l_3)
      assert(fsm_3.state == Follower)
      assert(fsm_3.followers.size == 0)
    } else {
      assert(fsm_3.state == Leader)
      assert(fsm_3.followers.size == 1)
      assert(fsm_3.followers.get(r_1).get(0) == l_2)
      assert(fsm_2.state == Follower)
      assert(fsm_2.followers.size == 0)
    }

    fsm_2.destroy()
    fsm_3.destroy()
  }

  it should "leader l_1 has zero replica when l_3 close" in {
    val myHandler = new QuorumHook {
      override private[core] def beLeader(): Unit = {}
      override private[core] def notLeader(): Unit = {}
      override private[core] def quorumConnect(evt: ConnectEvent): Unit = {}
      override private[core] def quorumClose(evt: CloseEvent): Unit = {}
      override private[core] def selfSequence(): Option[Long] = Some(_1)
      override private[core] def quorumSequence(quorumAddr: String): Option[Long] = Some(_1)
      override private[core] def rollback(correctSeq: Long): Boolean = true
    }
    val fsm_1 = new FSM(ZkConfig(zkAddress, zkNamespace, 300, 1000, 3), r_1, l_1, myHandler)
    val fsm_2 = new FSM(ZkConfig(zkAddress, zkNamespace, 300, 1000, 3), r_1, l_2, myHandler)
    val fsm_3 = new FSM(ZkConfig(zkAddress, zkNamespace, 300, 1000, 3), r_3, l_3, myHandler)

    fsm_1.init()
    fsm_2.init()
    fsm_3.init()

    fsm_3.destroy()

    Thread.sleep(200)
    assert(fsm_1.state == Leader)
    assert(fsm_1.followers.size == 0)
    assert(fsm_2.state == Follower)
    assert(fsm_2.followers.size == 0)

    fsm_1.destroy()
    fsm_2.destroy()
  }

  "r_1 mount l_1, r_2 mount l_2, r_3 mount l_3 and l_4" should "l_2 become leader and r_3 sync data to be follower when original leader l_1 close" in {
    var rollbackFlag = false
    val h_1 = new QuorumHook {
      override private[core] def beLeader(): Unit = {}
      override private[core] def notLeader(): Unit = {}
      override private[core] def quorumConnect(evt: ConnectEvent): Unit = {}
      override private[core] def quorumClose(evt: CloseEvent): Unit = {}
      override private[core] def selfSequence(): Option[Long] = Some(_1)
      override private[core] def quorumSequence(quorumAddr: String): Option[Long] = None
      override private[core] def rollback(correctSeq: Long): Boolean = true
    }
    val h_2 = new QuorumHook {
      override private[core] def beLeader(): Unit = {}
      override private[core] def notLeader(): Unit = {}
      override private[core] def quorumConnect(evt: ConnectEvent): Unit = {}
      override private[core] def quorumClose(evt: CloseEvent): Unit = {}
      override private[core] def selfSequence(): Option[Long] = Some(_2)
      override private[core] def quorumSequence(quorumAddr: String): Option[Long] = Some(_3)
      override private[core] def rollback(correctSeq: Long): Boolean = true
    }
    val h_3 = new QuorumHook {
      override private[core] def beLeader(): Unit = {}
      override private[core] def notLeader(): Unit = {}
      override private[core] def quorumConnect(evt: ConnectEvent): Unit = {}
      override private[core] def quorumClose(evt: CloseEvent): Unit = {}
      override private[core] def selfSequence(): Option[Long] = Some(_3)
      override private[core] def quorumSequence(quorumAddr: String): Option[Long] = Some(_2)
      override private[core] def rollback(correctSeq: Long): Boolean = { Thread.sleep(100); rollbackFlag = true; true }
    }

    val fsm_1 = new FSM(ZkConfig(zkAddress, zkNamespace, 300, 1000, 3), r_1, l_1, h_1)
    val fsm_2 = new FSM(ZkConfig(zkAddress, zkNamespace, 300, 1000, 3), r_2, l_2, h_2)
    val fsm_3 = new FSM(ZkConfig(zkAddress, zkNamespace, 300, 1000, 3), r_3, l_3, h_3)
    val fsm_4 = new FSM(ZkConfig(zkAddress, zkNamespace, 300, 1000, 3), r_3, l_4, h_3)

    fsm_1.init()
    fsm_2.init()
    fsm_3.init()
    fsm_4.init()

    fsm_1.destroy()
    Thread.sleep(300)

    assert(fsm_2.state == Leader)
    assert(fsm_2.followers.size == 1)
    assert(fsm_2.followers.get(r_3).get(0) == l_3 || fsm_2.followers.get(r_3).get(0) == l_4)
    assert(fsm_2.followers.get(r_3).get(1) == l_4 || fsm_2.followers.get(r_3).get(1) == l_3)
    assert(fsm_3.state == Follower)
    assert(fsm_3.followers.size == 0)
    assert(fsm_4.state == Follower)
    assert(fsm_4.followers.size == 0)
    assert(rollbackFlag)

    fsm_2.destroy()
    fsm_3.destroy()
    fsm_4.destroy()
  }

  it should "l_2 become leader even r_3 sync data too long to be follower when original leader l_1 close" in {
    val rollbackCount = new AtomicInteger(0)
    val h_1 = new QuorumHook {
      override private[core] def beLeader(): Unit = {}
      override private[core] def notLeader(): Unit = {}
      override private[core] def quorumConnect(evt: ConnectEvent): Unit = {}
      override private[core] def quorumClose(evt: CloseEvent): Unit = {}
      override private[core] def selfSequence(): Option[Long] = Some(_1)
      override private[core] def quorumSequence(quorumAddr: String): Option[Long] = None
      override private[core] def rollback(correctSeq: Long): Boolean = true
    }
    val h_2 = new QuorumHook {
      override private[core] def beLeader(): Unit = {}
      override private[core] def notLeader(): Unit = {}
      override private[core] def quorumConnect(evt: ConnectEvent): Unit = {}
      override private[core] def quorumClose(evt: CloseEvent): Unit = {}
      override private[core] def selfSequence(): Option[Long] = Some(_2)
      override private[core] def quorumSequence(quorumAddr: String): Option[Long] = Some(_3)
      override private[core] def rollback(correctSeq: Long): Boolean = true
    }
    val h_3 = new QuorumHook {
      override private[core] def beLeader(): Unit = {}
      override private[core] def notLeader(): Unit = {}
      override private[core] def quorumConnect(evt: ConnectEvent): Unit = {}
      override private[core] def quorumClose(evt: CloseEvent): Unit = {}
      override private[core] def selfSequence(): Option[Long] = Some(_3)
      override private[core] def quorumSequence(quorumAddr: String): Option[Long] = Some(_2)
      override private[core] def rollback(correctSeq: Long): Boolean = { Thread.sleep(300); rollbackCount.incrementAndGet(); true }
    }

    val fsm_1 = new FSM(ZkConfig(zkAddress, zkNamespace, 300, 1000, 3), r_1, l_1, h_1)
    val fsm_2 = new FSM(ZkConfig(zkAddress, zkNamespace, 300, 1000, 3), r_2, l_2, h_2)
    val fsm_3 = new FSM(ZkConfig(zkAddress, zkNamespace, 300, 1000, 3), r_3, l_3, h_3)
    val fsm_4 = new FSM(ZkConfig(zkAddress, zkNamespace, 300, 1000, 3), r_3, l_4, h_3)

    fsm_1.init()
    fsm_2.init()
    fsm_3.init()
    fsm_4.init()

    fsm_1.destroy()
    Thread.sleep(800)

    assert(fsm_2.state == Leader)
    assert(fsm_2.followers.size == 1)
    assert(fsm_2.followers.get(r_3).get(0) == l_3 || fsm_2.followers.get(r_3).get(0) == l_4)
    assert(fsm_2.followers.get(r_3).get(1) == l_4 || fsm_2.followers.get(r_3).get(1) == l_3)
    assert(fsm_3.state == Follower)
    assert(fsm_3.followers.size == 0)
    assert(fsm_4.state == Follower)
    assert(fsm_4.followers.size == 0)
    assert(rollbackCount.get == 2)

    fsm_2.destroy()
    fsm_3.destroy()
    fsm_4.destroy()
  }

  it should "discard l_3 and l_4 when leader delete r_3 from zk" in {
    val h_1 = new QuorumHook {
      override private[core] def beLeader(): Unit = {}
      override private[core] def notLeader(): Unit = {}
      override private[core] def quorumConnect(evt: ConnectEvent): Unit = {}
      override private[core] def quorumClose(evt: CloseEvent): Unit = {}
      override private[core] def selfSequence(): Option[Long] = Some(_1)
      override private[core] def quorumSequence(quorumAddr: String): Option[Long] = None
      override private[core] def rollback(correctSeq: Long): Boolean = true
    }
    val h_2 = new QuorumHook {
      override private[core] def beLeader(): Unit = {}
      override private[core] def notLeader(): Unit = {}
      override private[core] def quorumConnect(evt: ConnectEvent): Unit = {}
      override private[core] def quorumClose(evt: CloseEvent): Unit = {}
      override private[core] def selfSequence(): Option[Long] = Some(_2)
      override private[core] def quorumSequence(quorumAddr: String): Option[Long] = Some(_3)
      override private[core] def rollback(correctSeq: Long): Boolean = true
    }
    val h_3 = new QuorumHook {
      override private[core] def beLeader(): Unit = {}
      override private[core] def notLeader(): Unit = {}
      override private[core] def quorumConnect(evt: ConnectEvent): Unit = {}
      override private[core] def quorumClose(evt: CloseEvent): Unit = {}
      override private[core] def selfSequence(): Option[Long] = Some(_3)
      override private[core] def quorumSequence(quorumAddr: String): Option[Long] = Some(_2)
      override private[core] def rollback(correctSeq: Long): Boolean = true
    }

    val fsm_1 = new FSM(ZkConfig(zkAddress, zkNamespace, 300, 1000, 3), r_1, l_1, h_1)
    val fsm_2 = new FSM(ZkConfig(zkAddress, zkNamespace, 300, 1000, 3), r_2, l_2, h_2)
    val fsm_3 = new FSM(ZkConfig(zkAddress, zkNamespace, 300, 1000, 3), r_3, l_3, h_3)
    val fsm_4 = new FSM(ZkConfig(zkAddress, zkNamespace, 300, 1000, 3), r_3, l_4, h_3)

    fsm_1.init()
    fsm_2.init()
    fsm_3.init()
    fsm_4.init()

    fsm_2.discardRedis(r_3)
    Thread.sleep(200)

    assert(fsm_1.state == Leader)
    assert(fsm_2.state == Follower)
    assert(fsm_3.state == Discard)
    assert(fsm_4.state == Discard)
    assert(fsm_1.followers.size() == 1)
    assert(fsm_1.followers.get(r_2).get(0) == l_2)
  }
}
