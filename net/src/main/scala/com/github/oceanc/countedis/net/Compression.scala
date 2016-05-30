package com.github.oceanc.countedis.net

import net.jpountz.lz4.LZ4Factory
import org.slf4j.LoggerFactory
import org.xerial.snappy.Snappy

/**
 * @author chengyang
 */
object Compression {
  private val log = LoggerFactory.getLogger(Compression.getClass)

  private val factory = LZ4Factory.fastestInstance

  def compress(buf: Array[Byte])(implicit cp: CompressionProtocol): Option[Array[Byte]] = mkOption(cp, {
    case Lz4Compress => factory.fastCompressor.compress(buf)
    case SnappyCompress => Snappy.compress(buf)
  })

  def decompress(buf: Array[Byte], targetLen: Int)(implicit cp: CompressionProtocol): Option[Array[Byte]] = mkOption(cp ,{
    case Lz4Compress => factory.fastDecompressor.decompress(buf, targetLen)
    case SnappyCompress =>
      val result = new Array[Byte](targetLen)
      Snappy.uncompress(buf, 0, buf.length, result, 0)
      result
  })

  private def mkOption[A](cp: CompressionProtocol, fn: PartialFunction[CompressionProtocol, A]): Option[A] = try Some(fn(cp)) catch {
    case e: Throwable => log.error("codec error.", e); None
  }

}

sealed trait CompressionProtocol
case object Lz4Compress extends CompressionProtocol
case object SnappyCompress extends CompressionProtocol