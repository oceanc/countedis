package com.github.oceanc.countedis.net

import _root_.java.util.concurrent.atomic.AtomicInteger
import _root_.java.util.{ArrayList => JArrayList}

import com.github.oceanc.countedis.net.MockClasses._

/**
 * @author chengyang
 */
class MessageSpec extends UnitSpec {
  val code = new AtomicInteger(0)
  val mc = new MessageCodec

  "A MessageCodec use any codec protocol" should "codec Message which's content is null" in {
    val id = code.incrementAndGet.toShort
    mc.bind(Codec.spawn[Shop](id, Codec.Kryo, LgJava))// whatever codec is

    val msg = new Message(bizCode = id, content = null)
    val buf = mc.encode(msg)
    assert(buf.isDefined)

    val newMsg = mc.decode(buf.get)
    assert(newMsg.isEmpty)
  }

  "A MessageCodec use Kryo codec protocol by Scala which same as Java" should "codec Message which's content is a java bean" in {
    val id = code.incrementAndGet.toShort
    mc.bind(Codec.spawn[Shop](id, Codec.Kryo, LgScala))

    val msg = new Message(bizCode = id, content = shop)
    val buf = mc.encode(msg)
    assert(buf.isDefined)

    val newMsg = mc.decode[Shop](buf.get)
    val nshop = newMsg.get.content
    assert(newMsg.isDefined)
    assert(newMsg.get.traceId == msg.traceId)
    assert(newMsg.get.version == msg.version)
    assert(nshop.getName == shop.getName)
    assert(nshop.getOwner == shop.getOwner)
    assert(nshop.getProductions.size == shop.getProductions.size)
    assert(nshop.getProductions.get(0).getColor == shop.getProductions.get(0).getColor)
  }

  it should "codec Message which's content is a java list" in {
    val id = code.incrementAndGet.toShort
    mc.bind(Codec.spawn[JArrayList[Shirt]](id, Codec.Kryo, LgScala))

    val msg = new Message(bizCode = id, content = list)
    val buf = mc.encode(msg)
    assert(buf.isDefined)

    val newMsg = mc.decode[JArrayList[Shirt]](buf.get)
    val nlist = newMsg.get.content
    assert(newMsg.isDefined)
    assert(newMsg.get.traceId == msg.traceId)
    assert(newMsg.get.version == msg.version)
    assert(nlist.get(0).getColor == list.get(0).getColor)
    assert(nlist.get(0).getWeight == list.get(0).getWeight)
  }

  it should "codec Message which's content is a scala case class" in {
    val id = code.incrementAndGet.toShort
    mc.bind(Codec.spawn[GroupByMap](id, Codec.Kryo, LgScala))
    assertScala[GroupByMap](groupMap, id)
  }

  it should "codec Message which's content is a scala seq" in {
    val id = code.incrementAndGet.toShort
    mc.bind(Codec.spawn[List[Person]](id, Codec.Kryo, LgScala))
    assertScala[List[Person]]((0 until 200).map(_ => gary), id)
  }



  "A MessageCodec use Json codec protocol by Java" should "codec Message which's content is a java bean" in {
    val id = code.incrementAndGet.toShort
    mc.bind(Codec.spawn[Shop](id, Codec.Json, LgJava))

    val msg = new Message(bizCode = id, content = shop)
    val buf = mc.encode(msg)
    assert(buf.isDefined)

    val newMsg = mc.decode[Shop](buf.get)
    val nshop = newMsg.get.content
    assert(newMsg.isDefined)
    assert(newMsg.get.traceId == msg.traceId)
    assert(newMsg.get.version == msg.version)
    assert(nshop.getName == shop.getName)
    assert(nshop.getOwner == shop.getOwner)
    assert(nshop.getProductions.size == shop.getProductions.size)
    assert(nshop.getProductions.get(0).getColor == shop.getProductions.get(0).getColor)
  }

  it should "codec Message which's content is a java list" in {
    val id = code.incrementAndGet.toShort
    mc.bind(Codec.spawn[JArrayList[Shirt]](id, Codec.Json, LgJava))

    val msg = new Message(bizCode = id, content = list)
    val buf = mc.encode(msg)
    assert(buf.isDefined)

    val newMsg = mc.decode[JArrayList[Shirt]](buf.get)
    val nlist = newMsg.get.content
    assert(newMsg.isDefined)
    assert(newMsg.get.traceId == msg.traceId)
    assert(newMsg.get.version == msg.version)
    assert(nlist.get(0).getColor == list.get(0).getColor)
    assert(nlist.get(0).getWeight == list.get(0).getWeight)
  }



  "A MessageCodec use Json codec protocol by Scala" should "codec Message which's content is a java bean" in {
    val id = code.incrementAndGet.toShort
    mc.bind(Codec.spawn[Shirt](id, Codec.Json, LgScala))

    val msg = new Message(bizCode = id, content = shirtA)
    val buf = mc.encode(msg)
    assert(buf.isDefined)

    val newMsg = mc.decode[Shirt](buf.get)
    val nshirt = newMsg.get.content
    assert(newMsg.isDefined)
    assert(newMsg.get.traceId == msg.traceId)
    assert(newMsg.get.version == msg.version)
    assert(nshirt.getColor == shirtA.getColor)
    assert(nshirt.getLogo == shirtA.getLogo)
  }

  it should "codec Message which's content is a scala case class" in {
    val id = code.incrementAndGet.toShort
    mc.bind(Codec.spawn[GroupByMap](id, Codec.Json, LgScala))
    assertScala[GroupByMap](groupMap, id)
  }

  it should "codec Message which's content is a scala seq" in {
    val id = code.incrementAndGet.toShort
    mc.bind(Codec.spawn[List[Person]](id, Codec.Json, LgScala))
    assertScala[List[Person]]((0 until 200).map(_ => gary), id)
  }


  def assertScala[T](content: AnyRef, bizCode: Short): Unit = {
    val msg = new Message(bizCode = bizCode, content = content)
    val buf = mc.encode(msg)
    assert(buf.isDefined)
    println(buf.get.readableBytes)

    val newMsg = mc.decode[T](buf.get)
    assert(newMsg.isDefined)
    assert(newMsg.get.traceId == msg.traceId)
    assert(newMsg.get.version == msg.version)
    assert(newMsg.get.content == msg.content)
  }
}