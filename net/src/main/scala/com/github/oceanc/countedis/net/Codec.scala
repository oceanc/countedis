package com.github.oceanc.countedis.net

import _root_.java.io.StringWriter
import _root_.java.lang.reflect.InvocationHandler
import _root_.java.nio.charset.Charset
import _root_.java.text.SimpleDateFormat
import _root_.java.util
import _root_.java.util.{Collections, GregorianCalendar, TimeZone}

import com.alibaba.fastjson
import com.alibaba.fastjson.TypeReference
import com.alibaba.fastjson.parser.Feature
import com.alibaba.fastjson.serializer.SerializerFeature
import com.twitter.chill.{KryoBase, KryoPool, ScalaKryoInstantiator}
import de.javakaffee.kryoserializers._
import org.json4s._
import org.json4s.native.Serialization.read
import org.slf4j.LoggerFactory

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

/**
 * @author chengyang
 */
class Codec[T : Manifest](val id: Short, val protocol: Byte, val lg: Language, val clazz: Class[T], val tpeRef: TypeReference[T] = null) {
  import Codec._

  private val log = LoggerFactory.getLogger(getClass)
  private val hasTypeParams = clazz.getTypeParameters.nonEmpty

  implicit val fm = formats

  def encode(obj: T): Option[Array[Byte]] = mkOption(protocol, {
    case Kryo => kryoPool.toBytesWithoutClass(obj)
    case Json => fastjson.JSON.toJSONBytes(obj)
  }, {
    case Kryo => kryoPool.toBytesWithoutClass(obj)
    case Json => Extraction.decomposeWithBuilder(obj, JsonWriter.streaming(new StringWriter)).toString.getBytes(UTF_8)
  })

  def decode(buf: Array[Byte]): Option[T] = mkOption(protocol, {
    case Kryo =>
      kryoPool.fromBytes(buf, clazz)
    case Json =>
      if (hasTypeParams) fastjson.JSON.parseObject[T](new String(buf, UTF_8), tpeRef)
      else fastjson.JSON.parseObject[T](buf, clazz)
  }, {
    case Kryo => kryoPool.fromBytes(buf, clazz)
    case Json => read[T](new String(buf, UTF_8))
  })

  /*def typeEqual(m: Manifest[_]): Boolean = {
    val mf = implicitly[Manifest[T]]
    mf.equals(m)
  }*/

  private def mkOption[A](protocol: Byte, fnJava: PartialFunction[Byte, A], fnScala: PartialFunction[Byte, A]): Option[A] = try {
    val flag: Byte = (protocol & 0x0f).toByte
    lg match {
      case LgJava => Some(fnJava(flag))
      case lgScala => Some(fnScala(flag))
    }
  } catch {
    case e: Throwable => log.error("codec error.", e); None
  }

}

object Codec {
  val Kryo: Byte = 0x00
  val Json: Byte = 0x01
  val Fst: Byte = 0x02

  val CompressYes: Byte = 0x00
  val CompressNo: Byte = 0x10

  val JSON_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS"
  val UTF_8 = Charset.forName("UTF-8")

  def isCompress(b: Byte): Boolean = (b & 0xf0).toByte == CompressYes

  private val kryoPool = KryoPoolFactory.makePool(128, 16384) // 0.125k ~ 16k

  private val formats = new DefaultFormats {
    override def dateFormatter = {
      val f = new SimpleDateFormat(Codec.JSON_DATE_FORMAT)
      f.setTimeZone(TimeZone.getDefault)
      f
    }
  }

  // fastjson config
  fastjson.JSON.DEFAULT_GENERATE_FEATURE |= SerializerFeature.WriteNonStringKeyAsString.getMask
  fastjson.JSON.DEFAULT_GENERATE_FEATURE |= SerializerFeature.DisableCircularReferenceDetect.getMask
  fastjson.JSON.DEFAULT_GENERATE_FEATURE &= ~SerializerFeature.SortField.getMask
//  fastjson.JSON.DEFAULT_GENERATE_FEATURE &= ~SerializerFeature.QuoteFieldNames.getMask
  fastjson.JSON.DEFAULT_PARSER_FEATURE |= Feature.DisableCircularReferenceDetect.getMask
  fastjson.JSON.DEFFAULT_DATE_FORMAT = Codec.JSON_DATE_FORMAT


  def spawn[T](bizCode: Short, protocol: Byte, lg: Language): Codec[T] = macro impl[T]

  def impl[T: c.WeakTypeTag](c: blackbox.Context)(bizCode: c.Tree, protocol: c.Tree, lg: c.Tree) = {
    import c.universe._

    implicit val lift = Liftable[Language] {
      case LgScala => q"_root_.com.github.oceanc.countedis.net.LgScala"
      case LgJava => q"_root_.com.github.oceanc.countedis.net.LgJava"
    }

    val aType = implicitly[c.WeakTypeTag[T]].tpe
    val clazz = q"classOf[$aType]"
    val tpeRef = q"new com.alibaba.fastjson.TypeReference[$aType]() {}"

    q"new Codec[$aType]($bizCode, $protocol, $lg, $clazz, $tpeRef)"
  }


  private object KryoPoolFactory {
    private val mutex = new AnyRef
    @transient private var kpool: KryoPool = null

    def makePool(outBufferMin: Int,
                 outBufferMax: Int,
                 size: Int = Runtime.getRuntime.availableProcessors * 4): KryoPool = mutex.synchronized {
      if (null == kpool) {
        kpool = KryoPool.withBuffer(size, new MyScalaKryoInstantiator, outBufferMin, outBufferMax)
      }
      kpool
    }
  }

  private class MyScalaKryoInstantiator extends ScalaKryoInstantiator {
    override def newKryo(): KryoBase = {
      val k = super.newKryo()
      k.register(classOf[util.EnumMap[_ <: Enum[_], _]], new EnumMapSerializer)
      k.register(Collections.EMPTY_LIST.getClass, new CollectionsEmptyListSerializer)
      k.register(Collections.EMPTY_MAP.getClass, new CollectionsEmptyMapSerializer)
      k.register(Collections.EMPTY_SET.getClass, new CollectionsEmptySetSerializer)
      k.register(Collections.singletonList("").getClass, new CollectionsSingletonListSerializer)
      k.register(Collections.singleton("").getClass, new CollectionsSingletonSetSerializer)
      k.register(Collections.singletonMap("", "").getClass, new CollectionsSingletonMapSerializer)
      k.register(classOf[GregorianCalendar], new GregorianCalendarSerializer)
      k.register(classOf[InvocationHandler], new JdkProxySerializer)
      UnmodifiableCollectionsSerializer.registerSerializers(k)
      SynchronizedCollectionsSerializer.registerSerializers(k)
      k
    }
  }

}

trait Language
case object LgJava extends Language
case object LgScala extends Language
