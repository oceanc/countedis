package com.github.oceanc.countedis.net

import _root_.java.util.concurrent.ConcurrentHashMap

import io.netty.buffer.{ByteBuf, ByteBufUtil}
import io.netty.channel._
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.{LengthFieldBasedFrameDecoder, MessageToByteEncoder}
import io.netty.util.concurrent.{DefaultEventExecutorGroup, DefaultThreadFactory}
import org.slf4j.LoggerFactory

/**
 * @author chengyang
 */
class DefaultPipeline(mc: MessageCodec,
                      isClient: Boolean = false,
                      respond: NioRespond = null,
                      handlerThreadSize: Int = Runtime.getRuntime.availableProcessors() + 2) extends ChannelInitializer[SocketChannel] {
  assert(mc != null)

  private[this] val serverHandlers = new ConcurrentHashMap[Short, Handler[_]]
  private[this] val clientHandlers = new ConcurrentHashMap[Short, Handler[_]]
  private[this] val timeoutHandlers = new ConcurrentHashMap[Short, TimeoutHandler]

  def bind(id: Short, servHandler: Handler[_], cliHandler: Handler[_], timeoutHandler: TimeoutHandler) = {
    if (cliHandler != null) clientHandlers.put(id, cliHandler)
    if (servHandler != null) serverHandlers.put(id, servHandler)
    if (timeoutHandler != null) timeoutHandlers.put(id, timeoutHandler)
  }

  def bindServ(id: Short, servHandler: Handler[_]): Unit = if (servHandler != null) serverHandlers.put(id, servHandler)

  def bindCli(id: Short, cliHandler: Handler[_]): Unit = if (cliHandler != null) clientHandlers.put(id, cliHandler)

  def bindTimeout(id: Short, timeoutHandler: TimeoutHandler): Unit = if (timeoutHandler != null) timeoutHandlers.put(id, timeoutHandler)

  def get(id: Short, isClient: Boolean) = if (isClient) clientHandlers.get(id) else serverHandlers.get(id)

  def getTimeoutHandler(id: Short) = timeoutHandlers.get(id)

  override def initChannel(ch: SocketChannel): Unit = {
    ch.pipeline().addLast(new DecodeHandler(mc, Int.MaxValue))
    ch.pipeline().addLast(new EncodeHandler(mc))
    ch.pipeline().addLast(
      new DefaultEventExecutorGroup(handlerThreadSize, new DefaultThreadFactory("asyncPool")),
      new DefaultHandler(isClient, respond, get)
    )
  }
}

class EncodeHandler(val mc: MessageCodec) extends MessageToByteEncoder[Message[_]] {
  override def allocateBuffer(ctx: ChannelHandlerContext, msg: Message[_], preferDirect: Boolean): ByteBuf = {
    mc.encode(msg, ctx.alloc) match {
      case None      => throw new EncodeException("channel [" + ctx.channel.remoteAddress + "] message [" + msg + "] encode failed.")
      case Some(buf) => buf
    }
  }

  override def encode(ctx: ChannelHandlerContext, msg: Message[_], out: ByteBuf): Unit = {}
}

class DecodeHandler(val mc: MessageCodec, maxLen: Int = Int.MaxValue) extends LengthFieldBasedFrameDecoder(maxLen, 0, 4, 0, 0) {
  override def decode(ctx: ChannelHandlerContext, in: ByteBuf): AnyRef = {
    super.decode(ctx, in).asInstanceOf[ByteBuf] match {
      case null => null
      case buf  => mc.decode(buf) match {
        case None      =>
          buf.release()
          throw new DecodeException("channel [" + ctx.channel.remoteAddress + "] message [" + ByteBufUtil.hexDump(in) + "] decode failed.")
        case Some(msg) =>
          buf.release()
          msg
      }
    }
  }
}


class DefaultHandler(val isClient: Boolean = false,
                     val respond: NioRespond,
                     val b: (Short, Boolean) => Handler[_]) extends SimpleChannelInboundHandler[Message[_]] {
  private val log = LoggerFactory.getLogger(classOf[DefaultHandler])

  override def channelRead0(ctx: ChannelHandlerContext, msg: Message[_]): Unit = {
//    val remote = ctx.channel.remoteAddress
    b(msg.bizCode, isClient) match {
      case sa: ServerAccept   => sa handle msg
      case sr: ServerReply[_] => ctx writeAndFlush sr.handle(msg)
      case ca: ClientAccept   => if (respond.finish(msg)) ca handle msg
      case cr: ClientReply[_] => if (respond.finish(msg)) ctx writeAndFlush cr.handle(msg)
      case null               => if (isClient) respond.finish(msg) else log.warn("server side not define Handler for bizCode[{}]", msg.bizCode)
    }
  }

  @throws(classOf[Exception])
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause match {
      case e: DecodeException => log.error("decodeException...................", e)
      case e: EncodeException => log.error("encodeException...................", e)
      case _                  => super.exceptionCaught(ctx, cause)
    }
  }
}

sealed trait Handler[R] { def handle(msg: Message[_]): R }
trait ClientAccept   extends Handler[Unit]
trait ClientReply[R] extends Handler[Message[R]]
trait ServerAccept   extends Handler[Unit]
trait ServerReply[R] extends Handler[Message[R]]

trait TimeoutHandler { def handle(msg: Message[_]): Unit }

class DecodeException(message: String) extends Exception(message)
class EncodeException(message: String) extends Exception(message)