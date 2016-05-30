package com.github.oceanc.countedis.net

import com.github.oceanc.countedis.net.MockClasses._
import io.netty.buffer.{ByteBuf, PooledByteBufAllocator, Unpooled}
import io.netty.channel.ChannelOption
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.util.ResourceLeakDetector

/**
 * @author chengyang
 */
class MessageHandlerSpec extends UnitSpec {

  val beatles = "beatles"
  val mc = new MessageCodec
  ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.ADVANCED)

  "A MessageHandler handle Message object " should "via ServerAccept" in {
    val id = 9.toShort
    def findHandler(id: Short, b: Boolean): Handler[_] = new ServerAccept {
      override def handle(msg: Message[_]): Unit = msg.content.asInstanceOf[Shop].setName(beatles)
    }
    val msg = new Message(bizCode = id, content = shop)

    val channel = new EmbeddedChannel(new DefaultHandler(false, null, findHandler))
    channel.config().setOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)

    assertResult(false)(channel writeInbound msg)
    assertResult(false)(channel.finish)
    assert(null == channel.readInbound)
    assert(beatles == msg.content.getName)
  }

  it should "write something back via ServerReply" in {
    val id = 99.toShort
    val msg = new Message(bizCode = id, content = shop)
    val newMsg = Message(bizCode = msg.bizCode, shirtA)

    def findHandler(id: Short, b: Boolean): Handler[_] = new ServerReply[Shirt] {
      override def handle(msg: Message[_]): Message[Shirt] = newMsg
    }

    val channel = new EmbeddedChannel(new DefaultHandler(false, null, findHandler))
    channel.config().setOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)

    assertResult(false)(channel.writeInbound(msg))
    assert(channel.finish)
    assert(newMsg == channel.readOutbound)
  }

  "A EncodeHandler encode Message object " should "via eventLoop" in {
    val id = 1.toShort
    mc.bind(Codec.spawn[Shop](id, Codec.Kryo, LgScala))

    val msg = new Message(bizCode = id, content = shop)
    val channel = new EmbeddedChannel(new EncodeHandler(mc))

    assert(channel writeOutbound msg)
    assert(channel.finish)
    val newMsg = mc.decode[Shop](channel.readOutbound.asInstanceOf[ByteBuf]).get
    assert(msg.bizCode == newMsg.bizCode)
    assert(msg.traceId == newMsg.traceId)
    assert(msg.content.getName == newMsg.content.getName)
    assert(msg.content.getOwner == newMsg.content.getOwner)
  }

  "A DecodeHandler decode Message object " should "via eventLoop" in {
    val id_2 = 2.toShort
    mc.bind(Codec.spawn[Shop](id_2, Codec.Kryo, LgScala))

    val buf = mc.encode(new Message(bizCode = id_2, content = shop)).get
    val channel = new EmbeddedChannel(new DecodeHandler(mc))

    assert(channel writeInbound buf)
    assert(channel.finish)
    val newBuf = mc.encode[Shop](channel.readInbound.asInstanceOf[Message[Shop]]).get
    assert(buf == newBuf)
  }

  it should "log wrong bytes" in {
    val buf = Unpooled.copiedBuffer(Array[Byte](1,2,3,4,5,6,7,8,9,0))
    val channel = new EmbeddedChannel(new DecodeHandler(mc))

//    intercept[io.netty.handler.codec.DecoderException] {
      assertResult(false)(channel writeInbound buf)
      assertResult(false)(channel.finish)
//    }
  }

}
