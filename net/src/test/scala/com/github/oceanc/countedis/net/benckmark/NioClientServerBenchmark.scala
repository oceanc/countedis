package com.github.oceanc.countedis.net.benckmark

import com.github.oceanc.countedis.net.MockClasses._
import com.github.oceanc.countedis.net._
import org.scalameter.api._
import org.scalameter.picklers.Implicits._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
 * -Xmx2048m -Xms2048m -XX:MaxDirectMemorySize=1024m -Dio.netty.leakDetection.level=advanced
 * @author chengyang
 */
object NioClientServerBenchmark extends Bench.OfflineRegressionReport with Benchmark {

  private val log = LoggerFactory.getLogger(NioClientServerBenchmark.getClass)

  val host = "127.0.0.1"
  val port = 17890
  val address = host + ":" + port
  val mc = new MessageCodec

  override lazy val persistor: Persistor = new SerializationPersistor
  override lazy val executor = LocalExecutor(
    new Executor.Warmer.Default,
    Aggregator.min[Double],
    measurer)

  // ------------------------------------------------------------------------------
  val loop = Gen.range("loop")(250, 1000, 250).cached
  val size = Gen.enumeration("elements")(8, 16, 32, 64, 128, 256, 512).cached
  implicit val timeoutMs = 2000

  // -------------------------------------------------------scala kryo
  val shopIdScalaKryo = 1.toShort
  mc.bind(Codec.spawn[ShopScala](shopIdScalaKryo, Codec.Kryo, LgScala))
  Node.node.bindServ(shopIdScalaKryo, new ServerReply[ShopScala] {
    override def handle(msg: Message[_]): Message[ShopScala] = {
      val ss = msg.content.asInstanceOf[ShopScala]
      assert("CocaCola" == ss.name)
      assert("jack" == ss.owner)
      msg.asInstanceOf[Message[ShopScala]]
    }
  })

  val msgScalaKryo = (for (c <- size) yield {
    val shop = ShopScala("CocaCola", "jack", (0 to c).map(_ => ShirtScala("blue", "adidas", 20)))
    Message(bizCode = shopIdScalaKryo, content = shop)
  }).cached
  val msgScalaKryoLoop = Gen.crossProduct(loop, msgScalaKryo).cached

  // -------------------------------------------------------java kryo
  val shopIdJavaKryo = 2.toShort
  mc.bind(Codec.spawn[Shop](shopIdJavaKryo, Codec.Kryo, LgJava))
  Node.node.bindServ(shopIdJavaKryo, new ServerReply[Shop] {
    override def handle(msg: Message[_]): Message[Shop] = {
      val ss = msg.content.asInstanceOf[Shop]
      assert("CocaCola" == ss.getName)
      assert("jack" == ss.getOwner)
      msg.asInstanceOf[Message[Shop]]
    }
  })

  val msgJavaKryo = (for (c <- size) yield {
    val shop = new Shop("CocaCola", "jack", (0 to c).map(_ => new Shirt("blue", "adidas", 20)).asJava)
    Message(bizCode = shopIdJavaKryo, content = shop)
  }).cached
  val msgJavaKryoLoop = Gen.crossProduct(loop, msgJavaKryo).cached

  // -------------------------------------------------------java json
  val shopIdJavaJson = 3.toShort
  mc.bind(Codec.spawn[Shop](shopIdJavaJson, Codec.Json, LgJava))
  Node.node.bindServ(shopIdJavaJson, new ServerReply[Shop] {
    override def handle(msg: Message[_]): Message[Shop] = {
      val ss = msg.content.asInstanceOf[Shop]
      assert("CocaCola" == ss.getName)
      assert("jack" == ss.getOwner)
      msg.asInstanceOf[Message[Shop]]
    }
  })

  val msgJavaJson = (for (c <- size) yield {
    val shop = new Shop("CocaCola", "jack", (0 to c).map(_ => new Shirt("blue", "adidas", 20)).asJava)
    Message(bizCode = shopIdJavaJson, content = shop)
  }).cached
  val msgJavaJsonLoop = Gen.crossProduct(loop, msgJavaJson).cached


  performance of "Ask and answer" in {
    performance of "scala kryo" in {
      using(msgScalaKryoLoop) config (
        exec.independentSamples -> 1,
        exec.jvmflags -> List("-Xmx2048m", "-Xms2048m", "-XX:MaxDirectMemorySize=1024m", "-Dio.netty.leakDetection.level=advanced")
      ) in { case (lp, m) =>
        loopRun(lp, {
          try {
            val f = Node.node.ask(address, m).asInstanceOf[Future[ShopScala]]
            val ss = Await.result(f, 10.seconds)
            assert("CocaCola" == ss.name)
            assert("jack" == ss.owner)
          } catch {case e: Throwable => log.error("", e)}
        })
      }
    }

    performance of "java kryo" in {
      using(msgJavaKryoLoop) config (
        exec.independentSamples -> 1,
        exec.jvmflags -> List("-Xmx2048m", "-Xms2048m", "-XX:MaxDirectMemorySize=1024m", "-Dio.netty.leakDetection.level=advanced")
        ) in { case (lp, m) =>
        loopRun(lp, {
          try {
            val f = Node.node.ask(address, m).asInstanceOf[Future[Shop]]
            val ss = Await.result(f, 10.seconds)
            assert("CocaCola" == ss.getName)
            assert("jack" == ss.getOwner)
          } catch {case e: Throwable => log.error("", e)}
        })
      }
    }

    performance of "java json" in {
      using(msgJavaJsonLoop) config (
        exec.independentSamples -> 1,
        exec.jvmflags -> List("-Xmx2048m", "-Xms2048m", "-XX:MaxDirectMemorySize=1024m", "-Dio.netty.leakDetection.level=advanced")
        ) in { case (lp, m) =>
        loopRun(lp, {
          try {
            val f = Node.node.ask(address, m).asInstanceOf[Future[Shop]]
            val ss = Await.result(f, 10.seconds)
            assert("CocaCola" == ss.getName)
            assert("jack" == ss.getOwner)
          } catch {case e: Throwable => log.error("", e)}
        })
      }
    }
  }

  object Node {
    val node = new NioNode {
      override val attrs = Map.empty[String, AnyRef]
      override val localPort: Int = port
      override val servCodec: MessageCodec = mc
      override val cliCodec: MessageCodec = mc
    }
    node.start()
    node.connect(host, port)
  }
}