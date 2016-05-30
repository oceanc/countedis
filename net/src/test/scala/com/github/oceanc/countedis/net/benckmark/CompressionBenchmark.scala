package com.github.oceanc.countedis.net.benckmark

import com.github.oceanc.countedis.net.MockClasses._
import com.github.oceanc.countedis.net._
import org.scalameter.api._
import org.scalameter.persistence.SerializationPersistor
import org.scalameter.picklers.Implicits._

import scala.collection.JavaConverters._

/**
 * @author chengyang
 */
object CompressionBenchmark extends Bench.OfflineRegressionReport with Benchmark {

  override def persistor: Persistor = new SerializationPersistor

  val loop = Gen.range("loop")(1000, 4000, 1000).cached
  val size = Gen.enumeration("elements")(4, 8, 16, 32, 64, 128, 256, 512).cached

  val javaCodec = new Codec[Shop](1, Codec.Json, lg = LgJava, classOf[Shop])

  val beans = (for (c <- size) yield new Shop("CocaCola", "jack", (0 to c).map(_ => new Shirt("blue", "adidas", 20)).asJava)).cached
  val bytes = beans.map(s => javaCodec.encode(s).get).cached
  val bytesMetric = Gen.crossProduct(loop, bytes).cached

  val lz4BytesCompressedMetric = Gen.crossProduct(loop, bytes.map(s => (Compression.compress(s)(Lz4Compress).get, s.length)).cached).cached
  val snappyBytesCompressedMetric = Gen.crossProduct(loop, bytes.map(s => (Compression.compress(s)(SnappyCompress).get, s.length)).cached).cached

  performance of "Compression" in {
    // ------LZ4 compress
    performance of "LZ4 compress bytes" in {
      using(bytesMetric) config {
        exec.independentSamples -> 1
      } in { case (lp, e) =>
        loopRun(lp, Compression.compress(e)(Lz4Compress))
      }
    }

    // ------LZ4 decompress
    performance of "LZ4 decompress bytes" in {
      using(lz4BytesCompressedMetric) config {
        exec.independentSamples -> 1
      } in { case (lp, e) =>
        loopRun(lp, Compression.decompress(e._1, e._2)(Lz4Compress))
      }
    }

    // ------snappy compress
    performance of "Snappy compress bytes" in {
      using(bytesMetric) config {
        exec.independentSamples -> 1
      } in { case (lp, e) =>
        loopRun(lp, Compression.compress(e)(SnappyCompress))
      }
    }

    // ------snappy decompress
    performance of "Snappy decompress bytes" in {
      using(snappyBytesCompressedMetric) config {
        exec.independentSamples -> 1
      } in { case (lp, e) =>
        loopRun(lp, Compression.decompress(e._1, e._2)(SnappyCompress))
      }
    }

  }
}
