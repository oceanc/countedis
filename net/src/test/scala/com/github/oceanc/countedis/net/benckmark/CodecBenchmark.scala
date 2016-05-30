package com.github.oceanc.countedis.net.benckmark

import com.github.oceanc.countedis.net.MockClasses._
import com.github.oceanc.countedis.net.{Codec, LgJava, LgScala}
import org.scalameter.api._
import org.scalameter.picklers.Implicits._

import scala.collection.JavaConverters._

/**
 * @author chengyang
 */
object CodecBenchmark extends Bench.OfflineRegressionReport with Benchmark {

  override def persistor: Persistor = new SerializationPersistor

  val loop = Gen.range("loop")(250, 1000, 250).cached
  val size = Gen.enumeration("elements")(4, 8, 16, 32, 64).cached

  val javaJsonCodec = new Codec[Shop](1, Codec.Json, LgJava, classOf[Shop])
  val javaKryoCodec = new Codec[Shop](1, Codec.Kryo, LgJava, classOf[Shop])
  val scalaJsonCodec = new Codec[ShopScala](1, Codec.Json, LgScala, classOf[ShopScala])
  val scalaKryoCodec = new Codec[ShopScala](1, Codec.Kryo, LgScala, classOf[ShopScala])


  val javaBeanClass = new Shop(null, null, null).getClass

  // ------------- json: java bean classes
  val javaBeans = (for (c <- size) yield new Shop("CocaCola", "jack", (0 to c).map(_ => new Shirt("blue", "adidas", 20)).asJava)).cached
  val javaBeansMetric = Gen.crossProduct(loop, javaBeans).cached

  val jsonBytes = javaBeans.map(s => javaJsonCodec.encode(s).get).cached
  val jsonBytesMetric = Gen.crossProduct(loop, jsonBytes).cached

  // ------------- json: scala case classes
  val caseClasses = (for (c <- size) yield ShopScala("CocaCola", "jack", (0 to c).map(_ => ShirtScala("blue", "adidas", 20)))).cached
  val caseClassesMetric = Gen.crossProduct(loop, caseClasses).cached

  // ------------- kryo: java bean classes
  val kryoBeanBytes = javaBeans.map(s => javaKryoCodec.encode(s).get).cached
  val kryoBeanBytesTuple = Gen.crossProduct(loop, kryoBeanBytes).cached

  // ------------- kryo: scala case classes
  val kryoCaseBytes = caseClasses.map(s => scalaKryoCodec.encode(s).get).cached
  val kryoCaseBytesTuple = Gen.crossProduct(loop, kryoCaseBytes).cached


  performance of "Codec" in {
    // ------fastjson
    performance of "fastjson encode java bean" in {
      using(javaBeansMetric) config {
        exec.independentSamples -> 1
      } in { case (lp, e) =>
        loopRun(lp, javaJsonCodec.encode(e))
      }
    }

    performance of "fastjson decode java bean" in {
      using(jsonBytesMetric) config {
        exec.independentSamples -> 1
      } in { case (lp, e) =>
        loopRun(lp, javaJsonCodec.decode(e))
      }
    }

    // ------json4s
    performance of "json4s encode case class" in {
      using(caseClassesMetric) config {
        exec.independentSamples -> 1
      } in { case (lp, e) =>
        loopRun(lp, scalaJsonCodec.encode(e))
      }
    }

    performance of "json4s decode case class" in {
      using(jsonBytesMetric) config {
        exec.independentSamples -> 1
      } in { case (lp, e) =>
        loopRun(lp, scalaJsonCodec.decode(e))
      }
    }

    // ------kryo java bean
    performance of "kryo encode java bean" in {
      using(javaBeansMetric) config {
        exec.independentSamples -> 1
      } in { case (lp, e) =>
        loopRun(lp, javaKryoCodec.encode(e))
      }
    }

    performance of "kryo decode java bean" in {
      using(kryoBeanBytesTuple) config {
        exec.independentSamples -> 1
      } in { case (lp, e) =>
        loopRun(lp, javaKryoCodec.decode(e))
      }
    }

    // ------kryo case class
    performance of "kryo encode scala case class" in {
      using(caseClassesMetric) config {
        exec.independentSamples -> 1
      } in { case (lp, e) =>
        loopRun(lp, scalaKryoCodec.encode(e))
      }
    }

    performance of "kryo decode scala case class" in {
      using(kryoCaseBytesTuple) config {
        exec.independentSamples -> 1
      } in { case (lp, e) =>
        loopRun(lp, scalaKryoCodec.decode(e))
      }
    }
  }

}
