package com.github.oceanc.countedis.net

import _root_.java.util.concurrent.atomic.AtomicInteger
import _root_.java.util.{ArrayList => JArrayList}

import com.github.oceanc.countedis.net.MockClasses._

import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * @author chengyang
 */
class NioClientServerSpec extends UnitSpec {
  val beatles = "beatles"
  val code = new AtomicInteger(0)
  implicit val timeoutMs = 600

  val host = "127.0.0.1"
  val port = 7890
  val address = host + ":" + port
  val mc = new MessageCodec

  val client = new NioClient {
    override val attrs: Map[String, AnyRef] = Map.empty
    override val cliCodec: MessageCodec = mc
  }
  val server = new NioServer {
    val attrs: Map[String, AnyRef] = Map.empty
    val localPort: Int = port
    override val servCodec: MessageCodec = mc
  }

  server.start()
  client.connect(host, port)


  "A NioNode instance" should "send message and received by server side" in {
    var newShop: Shop = null
    val id = code.incrementAndGet.toShort // for Shop class kryo
    // for test case 1 - test no respond
    mc.bind(Codec.spawn[Shop](id, Codec.Kryo, LgScala))
    server.bindServ(id, new ServerAccept {
      override def handle(msg: Message[_]): Unit = {
        newShop = msg.content.asInstanceOf[Shop]
        newShop.setName(beatles)
      }
    })

    client.tell(address, Message(bizCode = id, content = shop))

    Thread.sleep(1000)

    assert(beatles == newShop.getName)
    assert(shop.getOwner == newShop.getOwner)
  }


  it should "send message then async receive respond from server side" in {
    var newShirt: Shirt = null
    val id_1 = code.incrementAndGet.toShort // for Shop class json
    val id_2 = code.incrementAndGet.toShort // for Shirt class json
    // for test case 2 - test normal respond
    mc.bind(Codec.spawn[Shop](id_1, Codec.Json, LgJava))
    mc.bind(Codec.spawn[Shirt](id_2, Codec.Json, LgJava))
    server.bindServ(id_1, new ServerReply[Shirt] {
      override def handle(msg: Message[_]): Message[Shirt] = {
        new Message(traceId = msg.traceId, bizCode = id_2, content = shirtA)
      }
    })
    client.bindCli(id_2, new ClientAccept {
      override def handle(msg: Message[_]): Unit = {
        newShirt = msg.content.asInstanceOf[Shirt]
        newShirt.setLogo(beatles)
      }
    })

    client.askAsync(address, Message(bizCode = id_1, content = shop))

    Thread.sleep(1900)

    assert(beatles == newShirt.getLogo)
    assert(shirtA.getColor == newShirt.getColor)
    assert(shirtA.getWeight == newShirt.getWeight)
  }


  it should "send message then async trigger timeout handler when server respond slowly" in {
    var timeoutExecute: Boolean = false
    val id = code.incrementAndGet.toShort // for GroupBySeq class kryo
    // for test case 3 - test timeout
    mc.bind(Codec.spawn[GroupBySeq](id, Codec.Kryo, LgScala))
    server.bindServ(id, new ServerReply[GroupBySeq] {
      override def handle(msg: Message[_]) = {
        Thread.sleep(1000)
        Message(bizCode = msg.bizCode, content = groupSeq, traceId = msg.traceId)
      }
    })
    client.bindTimeout(id, new TimeoutHandler {
      override def handle(msg: Message[_]): Unit = timeoutExecute = true
    })

    client.askAsync(address, Message(bizCode = id, content = groupSeq))

    Thread.sleep(1500)

    assert(timeoutExecute)
  }


  it should "send message and get future object to receive respond " in {
    val id = code.incrementAndGet.toShort // for JArrayList[Shirt] class kryo
    // for test case 4 - test future
    mc.bind(Codec.spawn[JArrayList[Shirt]](id, Codec.Kryo, LgScala))
    server.bindServ(id, new ServerReply[JArrayList[Shirt]] {
      override def handle(msg: Message[_]): Message[JArrayList[Shirt]] = {
        val shirts = msg.content.asInstanceOf[JArrayList[Shirt]]
        shirts.clear()
        shirts.add(shirtA)
        Message(bizCode = msg.bizCode, content = shirts, traceId = msg.traceId)
      }
    })

    val future = client.ask[JArrayList[Shirt]](address, Message(bizCode = id, content = list))(1000)

    //    var newList: JArrayList[Shirt] = null
    //    future.onSuccess{ case xx: JArrayList[Shirt] => newList= xx }
    val newList = Await.result(future, 10.seconds)

    assert(newList.size == 1)
    assert(newList.get(0).getColor == shirtA.getColor)
    assert(newList.get(0).getLogo == shirtA.getLogo)
  }


  it should "send message and get future timeout" in {
    val future = client.ask[JArrayList[Shirt]](address, Message(bizCode = 11111, content = list))

    intercept[com.github.oceanc.countedis.net.AskTimeoutException] {
      Await.result(future, 1.seconds)
    }
  }
}
