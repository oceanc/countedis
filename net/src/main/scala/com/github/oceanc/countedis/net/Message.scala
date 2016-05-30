package com.github.oceanc.countedis.net

import _root_.java.util.concurrent.ConcurrentHashMap
import _root_.java.util.concurrent.atomic.AtomicInteger

import io.netty.buffer.{ByteBuf, ByteBufAllocator, PooledByteBufAllocator}

//import scala.language.experimental.macros

/**
 * message object for internal communication
 * @param bizCode unique code for message content that means business
 * @param content content object
 * @param traceId unique id for track
 * @author chengyang
 */
case class Message[T](bizCode: Short, content: T, traceId: Int = Message.traceIdGen.incrementAndGet) {
  val version: Byte = Message.VERSION_1
  def this(bizCode: Short, content: T) = this(bizCode, content, traceId = Message.traceIdGen.incrementAndGet)
}

object Message {
  val traceIdGen = new AtomicInteger(0)
  val criticalBytesLength = 4096
  val VERSION_1: Byte = 0x01
}

class MessageCodec {
  import Message._
  private val log = org.log4s.getLogger
  implicit val compressor = Lz4Compress
  private val supported = new ConcurrentHashMap[Short, Codec[_]]
  /** version + cc + bizCode + traceId + actualLen **/
  private val fixedBytesLength = 1 + 1 + 2 + 4 + 4

  def encode[T](msg: Message[T], alloc: ByteBufAllocator = PooledByteBufAllocator.DEFAULT): Option[ByteBuf] = {
    val cdc = supported.get(msg.bizCode).asInstanceOf[Codec[T]]
    val contentBytes = if (msg.content == null) Some(Array.empty[Byte]) else cdc.encode(msg.content)

    val (bytes, cc, fixedLen) = contentBytes match {
      case None => (None, 0.toByte, fixedBytesLength)
      case Some(bs) =>
        if (criticalBytesLength <= 0 || criticalBytesLength > bs.length)
          (Some(bs), (Codec.CompressNo | cdc.protocol).toByte, fixedBytesLength)
        else
          (Compression.compress(bs), (Codec.CompressYes | cdc.protocol).toByte, fixedBytesLength + 4)
    }

    bytes match {
      case None =>
        log.info("encode message failed.")
        None
      case Some(bs) =>
        val buf = alloc.buffer(4 + bs.length + fixedLen)
        buf.writeInt(bs.length + fixedLen) //content length
        buf.writeBytes(Array(msg.version, cc)) //version + compress&codec
        buf.writeShort(msg.bizCode) //bizCode
        buf.writeInt(msg.traceId) //traceId
        buf.writeInt(bs.length) //actualLen
        if (Codec.isCompress(cc)) buf.writeInt(contentBytes.get.length) //originalLen
        buf.writeBytes(bs) //content
        Some(buf)
    }
  }

  def decode[T](buf: ByteBuf): Option[Message[T]] = {
    try {
      val totalLen = buf.readInt()
      log.trace(s"Message total length is $totalLen")
      val Array(_, cc) = buf.readBytes(2).array //version + compress&codec
      val bizCode = buf.readShort //bizCode
      val tId = buf.readInt //traceId
      val aLen = buf.readInt //actualLen
      val oBytes = Codec.isCompress(cc) match {
          case true =>
            val oLen = buf.readInt //originalLen
            Compression.decompress(buf.readBytes(aLen).array, oLen)
          case false => Some(buf.readBytes(aLen).array)
        }

      val obj = oBytes match {
        case None => None
        case Some(bs) => if (bs.isEmpty) Some(null) else supported.get(bizCode).decode(bs)
      }

      obj match {
        case None => None
        case Some(x) => if (x == null) None else Some(Message[T](bizCode, x.asInstanceOf[T], traceId = tId))
      }
    } catch {
      case e: Throwable =>
        log.error(e)("decode to Message instance failed.")
        None
    }
  }

  /*def bind[T](id: Short, codecProtocol: Byte, lg: Language): Unit = macro implBind[T]*/

  def bind(cdc: Codec[_]): Unit = supported.put(cdc.id, cdc)

  /*def implBind[T: c.WeakTypeTag](c: blackbox.Context)(id: c.Tree, codecProtocol: c.Tree, lg: c.Tree) = {
    import c.universe._
    val cdc = Codec.impl(c)(id, codecProtocol, lg)
    q"new _root_.com.github.oceanc.countedis.net.Message.MessageConfig($cdc)"
  }

  class MessageConfig(val cdc: Codec[_] = null) {
    if (cdc != null) supported.put(cdc.id, cdc)
  }*/

}
