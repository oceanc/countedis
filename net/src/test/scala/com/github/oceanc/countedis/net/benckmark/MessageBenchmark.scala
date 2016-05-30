package com.github.oceanc.countedis.net.benckmark

import _root_.java.util.concurrent.atomic.AtomicInteger

import com.github.oceanc.countedis.net.MockClasses._
import com.github.oceanc.countedis.net._
import org.scalameter.api._
import org.scalameter.picklers.Implicits._

import scala.collection.JavaConverters._

/**
 * @author chengyang
 */
object MessageBenchmark extends Bench.OfflineRegressionReport with Benchmark {

  override def persistor: Persistor = new SerializationPersistor

  val id = new AtomicInteger(1)
  val mc = new MessageCodec

  val shopIdJavaJson = id.incrementAndGet.toShort
  mc.bind(Codec.spawn[Shop](shopIdJavaJson, Codec.Json, LgJava))

  val shopIdScalaJson = id.incrementAndGet.toShort
  mc.bind(Codec.spawn[ShopScala](shopIdScalaJson, Codec.Json, LgScala))

  val shopIdJavaKryo = id.incrementAndGet.toShort
  mc.bind(Codec.spawn[Shop](shopIdJavaKryo, Codec.Kryo, LgJava))

  val shopIdScalaKryo = id.incrementAndGet.toShort
  mc.bind(Codec.spawn[ShopScala](shopIdScalaKryo, Codec.Kryo, LgScala))

  // ------------------------------------------------------------------------------
  val loop = Gen.range("loop")(250, 1000, 250).cached
  val size = Gen.enumeration("elements")(8, 16, 32, 64, 128, 256, 512).cached

  // -------------------------------------------------------bean classes
  val msgJavaJson = (for (c <- size) yield {
    val shop = new Shop("CocaCola", "jack", (0 to c).map(_ => new Shirt("blue", "adidas", 20)).asJava)
    new Message(bizCode = shopIdJavaJson, content = shop)
  }).cached
  val msgJavaKryo = (for (c <- size) yield {
    val shop = new Shop("CocaCola", "jack", (0 to c).map(_ => new Shirt("blue", "adidas", 20)).asJava)
    new Message(bizCode = shopIdJavaKryo, content = shop)
  }).cached
  // -------------------------------------------------------case classes
  val msgScalaJson = (for (c <- size) yield {
    val shop = ShopScala("CocaCola", "jack", (0 to c).map(_ => ShirtScala("blue", "adidas", 20)))
    new Message(bizCode = shopIdScalaJson, content = shop)
  }).cached
  val msgScalaKryo = (for (c <- size) yield {
    val shop = ShopScala("CocaCola", "jack", (0 to c).map(_ => ShirtScala("blue", "adidas", 20)))
    new Message(bizCode = shopIdScalaKryo, content = shop)
  }).cached

  val msgJavaJsonBytes = msgJavaJson.map(s => mc.encode(s).get).cached
  val msgScalaJsonBytes = msgScalaJson.map(s => mc.encode(s).get).cached
  val msgJavaKryoBytes = msgJavaKryo.map(s => mc.encode(s).get).cached
  val msgScalaKryoBytes = msgScalaKryo.map(s => mc.encode(s).get).cached

  // ------------------------------------------------------------------------------
  val msgJavaJsonEncode = Gen.crossProduct(loop, msgJavaJson).cached
  val msgScalaJsonEncode = Gen.crossProduct(loop, msgScalaJson).cached
  val msgJavaKryoEncode = Gen.crossProduct(loop, msgJavaKryo).cached
  val msgScalaKryoEncode = Gen.crossProduct(loop, msgScalaKryo).cached

  val msgJavaJsonBytesDecode = Gen.crossProduct(loop, msgJavaJsonBytes).cached
  val msgScalaJsonBytesDecode = Gen.crossProduct(loop, msgScalaJsonBytes).cached
  val msgJavaKryoBytesDecode = Gen.crossProduct(loop, msgJavaKryoBytes).cached
  val msgScalaKryoBytesDecode = Gen.crossProduct(loop, msgScalaKryoBytes).cached

  performance of "Message encode" in {
    // ------json java
    performance of "fastjson encode java bean" in {
      using(msgJavaJsonEncode) config {
        exec.independentSamples -> 1
      } in { case (lp, m) =>
        loopRun(lp, mc.encode(m))
      }
    }
    // ------json scala
    /*performance of "json4s encode scala case class" in {
      using(msgScalaJsonEncode) config {
        exec.independentSamples -> 1
      } in { case (lp, m) =>
        loopRun(lp, codec.encode(m))
      }
    }*/
    // ------kryo java
    performance of "kryo encode java bean" in {
      using(msgJavaKryoEncode) config {
        exec.independentSamples -> 1
      } in { case (lp, m) =>
        loopRun(lp, mc.encode(m))
      }
    }
    // ------kryo scala
    performance of "kryo encode scala case class" in {
      using(msgScalaKryoEncode) config {
        exec.independentSamples -> 1
      } in { case (lp, m) =>
        loopRun(lp, mc.encode(m))
      }
    }

  }

  performance of "Message decode" in {
    // ------json java
    performance of "fastjson decode java bean" in {
      using(msgJavaJsonBytesDecode) config {
        exec.independentSamples -> 1
      } in { case (lp, m) =>
        loopRun(lp, {
          m.resetReaderIndex
          mc.decode(m)
        })
      }
    }
    // ------scala json4s
    /*performance of "json4s decode scala case class" in {
      using(msgScalaJsonBytesDecode) config {
        exec.independentSamples -> 1
      } in { case (lp, m) =>
        loopRun(lp, {
          m.resetReaderIndex
          codec.decode(m)
        })
      }
    }*/
    // ------kryo java
    performance of "kryo decode java bean" in {
      using(msgJavaKryoBytesDecode) config {
        exec.independentSamples -> 1
      } in { case (lp, m) =>
        loopRun(lp, {
          m.resetReaderIndex
          mc.decode(m)
        })
      }
    }
    // ------kryo scala
    performance of "kryo decode scala case class" in {
      using(msgScalaKryoBytesDecode) config {
        exec.independentSamples -> 1
      } in { case (lp, m) =>
        loopRun(lp, {
          m.resetReaderIndex
          mc.decode(m)
        })
      }
    }
  }
}
