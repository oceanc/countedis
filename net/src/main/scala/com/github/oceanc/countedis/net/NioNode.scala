package com.github.oceanc.countedis.net

import _root_.java.net.InetSocketAddress
import _root_.java.util.concurrent._

import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.netty.bootstrap.{Bootstrap, ServerBootstrap}
import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.{NioServerSocketChannel, NioSocketChannel}
import io.netty.channel.{Channel, ChannelOption}
import io.netty.util.AttributeKey

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.language.existentials

/**
 * encapsulate [[com.github.oceanc.countedis.net.NioServer]]
 * and [[com.github.oceanc.countedis.net.NioClient]] for convenience
 * @author chengyang
 */
trait NioNode extends NioServer with NioClient {
  def close(): Unit = {
    super.closeAllClient()
    super.closeServer()
  }
}

trait NioServer {
  val servCodec: MessageCodec
  val attrs: Map[String, AnyRef]
  val localPort: Int

  private val log = org.log4s.getLogger
  private val bossGroup = new NioEventLoopGroup
  private val workerGroup = new NioEventLoopGroup
  private[this] var channel: Channel = null
  private lazy val pipeline = new DefaultPipeline(servCodec)

  def bindServ(id: Short, servHandler: Handler[_]): Unit = pipeline.bindServ(id, servHandler)

  def start(): Unit = {
    val b: ServerBootstrap = new ServerBootstrap
    b.group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])
//      .handler(new LoggingHandler(LogLevel.INFO))
      .childHandler(pipeline)
      .option(ChannelOption.TCP_NODELAY, Boolean.box(true))
      .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
      .option[java.lang.Integer](ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
      .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
      .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
    for (att <- attrs) {
      b.attr(AttributeKey.valueOf(att._1), att._2)
      log.info(s"server for node communication add global attribute ${att._1}: ${att._2}")
    }
    val future = b.bind(localPort).syncUninterruptibly
    log.info(s"server for node communication started on port $localPort ......")
    channel = future.channel
  }

  def closeServer(): Unit = {
    if (channel != null) {
      channel.close()//.closeFuture.sync
      bossGroup.shutdownNow()//.shutdownGracefully().sync
      workerGroup.shutdownNow()//.shutdownGracefully().sync
    }
  }
}

trait NioClient {
  val cliCodec: MessageCodec
  val attrs: Map[String, AnyRef]

  private val log = org.log4s.getLogger
  private[this] val respond = new NioRespond(this.getTimeoutHandler)
  private[this] val channels = new ConcurrentHashMap[String, Channel](2)
  private val group = new NioEventLoopGroup
  private lazy val pipeline = new DefaultPipeline(cliCodec, true, respond)

  def bindCli(id: Short, cliHandler: Handler[_]): Unit = pipeline.bindCli(id, cliHandler)

  def bindTimeout(id: Short, timeoutHandler: TimeoutHandler): Unit = pipeline.bindTimeout(id, timeoutHandler)

  def getTimeoutHandler(id: Short): TimeoutHandler = pipeline.getTimeoutHandler(id)

  def connect(host: String, port: Int, ephemeral: Boolean = false) = {
    val b = new Bootstrap
    b.group(group)
      .channel(classOf[NioSocketChannel])
      .remoteAddress(new InetSocketAddress(host, port))
      .handler(pipeline)
      .option(ChannelOption.TCP_NODELAY, Boolean.box(true))
      .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
      .option[java.lang.Integer](ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
      .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)

    for (att <- attrs) {
      b.attr(AttributeKey.valueOf(att._1), att._2)
      log.info(s"client for node communication add global attribute ${att._1}: ${att._2}")
    }

    val f = b.connect.sync
    log.info(s"build connection to $host:$port successful.")
    if(!ephemeral) channels.put(host + ":" + port, f.channel)
    f.channel
  }

  /**
   * @param address e.g 127.0.0.1:7891
   */
  def closeClient(address: String): Unit = {
    channels.get(address) match {
      case null    => log.info(s"close connection of remote $address failed cause the remote address do not exist")
      case channel => channel.close()//.sync
    }
  }

  def closeAllClient(): Unit = {
    for(c <- channels.values()) c.close()//.closeFuture.sync
    group.shutdown()//.shutdownGracefully().sync
    respond.killMe()
  }

  def ask[T](address: String, msg: Message[_])(implicit timeMs: Int): Future[T] = ask[T](address, msg, channels.get(address))

  def ask[T](address: String, msg: Message[_], channel: Channel)(implicit timeMs: Int): Future[T] = {
    tell(address, msg, channel)
    respond.promise(msg, timeMs, uh = false)
  }

  def askAsync(address: String, msg: Message[_])(implicit timeMs: Int): Unit = askAsync(address, msg, channels.get(address))

  def askAsync(address: String, msg: Message[_], channel: Channel)(implicit timeMs: Int): Unit = {
    tell(address, msg, channel)
    respond.promise(msg, timeMs, uh = true)
  }

  def tell(address: String, msg: Message[_]): Unit = tell(address, msg, channels.get(address))

  def tell(address: String, msg: Message[_], channel: Channel): Unit = {
    channel writeAndFlush msg
  }
}


class NioRespond(th: (Short) => TimeoutHandler) {
  private val log = org.log4s.getLogger
  @volatile private var alive = true
  private val shardingSize = 11
  private val cleaner = Executors.newFixedThreadPool(shardingSize, new ThreadFactoryBuilder().setNameFormat("Receiving-cleaner-%d").build)
  private val receivingMap = new ConcurrentHashMap[Int, PromiseCallback[_]]
  private val receivingQs = mutable.Map.empty[Int, DelayQueue[Receiving]]

  (0 until shardingSize).foreach(i => receivingQs += (i -> new DelayQueue[Receiving]))

  (0 until shardingSize).foreach{i =>
    cleaner.submit(new Runnable {
      override def run(): Unit = {
        log.debug(s"thread ${Thread.currentThread.getName} start cleaning expired msg of Queue[$i].")
        while(alive) {
          try {
            val expired = receivingQs(i).take
            val p = receivingMap.remove(expired.idx)
            if (p != null && p.msg != null) {
              log.info(s"expired message ${p.msg}")
              val handler = th(p.msg.bizCode)
              if (handler != null) handler.handle(p.msg)
              //else log.warn("timeout handler of bizCode={} is not defined.", p.msg.bizCode)
              else p.p.failure(new AskTimeoutException("ask " + p.msg + " timeout(" + expired.timeoutMillis + ")ms."))
            }
          } catch {
            case _: InterruptedException => log.debug("clean expired msg InterruptedException.")
            case e: Throwable            => log.error(e)("clean expired msg error.")
          }
        }
        log.debug(s"thread ${Thread.currentThread.getName} stop cleaning expired msg of Queue[$i].")
      }
    })
  }

  def promise[R](msg: Message[_], timeoutMillis: Int, uh: Boolean): Future[R] = {
    val p = Promise[R]()
    //val aMsg = if (uh) msg else null
    receivingMap.put(msg.traceId, PromiseCallback[R](p, msg, useHandler = uh))
    locateQ(msg.traceId).offer(new Receiving(msg.traceId, timeoutMillis))
    p.future
  }

  def finish(msg: Message[_]): Boolean = {
    receivingMap.remove(msg.traceId) match {
      case null => log.info(s"respond=$msg is missing."); false
      case pc =>
        val ok = locateQ(msg.traceId).remove(new Receiving(msg.traceId, 0))
        if (!ok) log.debug(s"receiving object traceId=${msg.traceId} not exist in queue")
        val p = pc.asInstanceOf[PromiseCallback[Any]]
        p.p.success(msg.content)
        p.useHandler
    }
  }

  def killMe() = {
    alive = false
    cleaner.shutdownNow()
  }

  private def locateQ(id: Int): DelayQueue[Receiving] = receivingQs(Math.abs(id % shardingSize))

  case class PromiseCallback[R](p: Promise[R], msg: Message[_], useHandler: Boolean = false)

  class Receiving(val idx: Int, val timeoutMillis: Int) extends Delayed {

    private val expiredTimestamp = System.currentTimeMillis + timeoutMillis

    override def getDelay(unit: TimeUnit): Long = expiredTimestamp - System.currentTimeMillis

    override def compareTo(o: Delayed): Int = this.getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS) match {
      case 0          => 0
      case x if x > 0 => 1
      case _          => -1
    }

    override def hashCode(): Int = idx.hashCode

    override def equals(obj: scala.Any): Boolean = {
      if (obj == null) false
      else if (getClass != obj.getClass) false
      else obj.asInstanceOf[Receiving].idx == idx
    }
  }
}

class AskTimeoutException(message: String) extends Exception(message)