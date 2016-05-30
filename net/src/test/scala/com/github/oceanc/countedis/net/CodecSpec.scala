package com.github.oceanc.countedis.net

import _root_.java.util.{ArrayList => JArrayList, HashMap => JHashMap, HashSet => JHashSet}

import com.github.oceanc.countedis.net.MockClasses._

/**
 * @author chengyang
 */
class CodecSpec extends UnitSpec {

  "A FastJsonCodec instance" should "codec simple object with bytes" in {
    val shirtCodec = Codec.spawn[Shirt](1, Codec.Json, LgJava)
    val bytes = shirtCodec.encode(shirtA)
    assert(bytes.isDefined)

    val result = shirtCodec.decode(bytes.get)
    assert(result.isDefined)
    assert(shirtA.getColor == result.get.getColor)
  }

  it should "codec java list consist of simple object with bytes" in {
    val listCodec = Codec.spawn[JArrayList[Shirt]](1, Codec.Json, LgJava)
    val bytes = listCodec.encode(list)
    assert(bytes.isDefined)

    val result = listCodec.decode(bytes.get)
    assert(result.isDefined)
//    intercept[java.lang.ClassCastException] {
    assert(list.get(0).getColor == result.get.get(0).getColor)
    assert(list.get(1).getColor == result.get.get(1).getColor)
    assert(list.get(2).getColor == result.get.get(2).getColor)
//    }
  }

  it should "codec composite object with bytes" in {
    val shopCodec = Codec.spawn[Shop](1, Codec.Json, LgJava)
    val bytes = shopCodec.encode(shop)
    assert(bytes.isDefined)

    val result = shopCodec.decode(bytes.get)
    assert(result.isDefined)
    assert(shop.getName == result.get.getName)
    assert(shop.getProductions.get(0).getColor == result.get.getProductions.get(0).getColor)
    assert(shop.getProductions.get(1).getColor == result.get.getProductions.get(1).getColor)
    assert(shop.getProductions.get(2).getColor == result.get.getProductions.get(2).getColor)
  }


  "A Json4sCodec instance" should "codec simple object with bytes" in assertCodec(shoe, Codec.spawn[Shoe](1, Codec.Json, LgScala))

  it should "codec composite object with bytes" in assertCodec(gary, Codec.spawn[Person](1, Codec.Json, LgScala))

  it should "codec Array consist of simple object with bytes" in assertCodec(Array(shoe), Codec.spawn[Array[Shoe]](1, Codec.Json, LgScala))

  it should "codec Array consist of composite object with bytes" in assertCodec(Array(jack), Codec.spawn[Array[Person]](1, Codec.Json, LgScala))

  it should "codec Seq consist of simple object with bytes" in assertCodec(List(jack), Codec.spawn[List[Person]](1, Codec.Json, LgScala))

  it should "codec Set consist of simple object with bytes" in assertCodec(Set(jack), Codec.spawn[Set[Person]](1, Codec.Json, LgScala))

  it should "codec composite object contains Seq field with bytes" in assertCodec(groupSeq, Codec.spawn[GroupBySeq](1, Codec.Json, LgScala))

  it should "codec composite object contains Map field with bytes" in assertCodec(groupMap, Codec.spawn[GroupByMap](1, Codec.Json, LgScala))

  it should "codec composite object contains Set field with bytes" in assertCodec(groupSet, Codec.spawn[GroupBySet](1, Codec.Json, LgScala))

  it should "codec simple java bean" in {
    val cdc = Codec.spawn[Shirt](1, Codec.Json, LgScala)
    val bytes = cdc.encode(shirtA)
    assert(bytes.isDefined)
//    println(bytes.get.length)

    val newObj = cdc.decode(bytes.get)
    assert(newObj.isDefined)
    assert(shirtA.getColor == newObj.get.getColor)
    assert(shirtA.getLogo == newObj.get.getLogo)
    assert(shirtA.getWeight == newObj.get.getWeight)
  }

  //this will fail case json4s cant decode java collection
  it should "codec composite java bean" in {
    val cdc = Codec.spawn[Shop](1, Codec.Json, LgScala)
    val bytes = cdc.encode(shop)
    assert(bytes.isDefined)
//    println(bytes.get.length)
    println(new String(bytes.get))

    val newObj = cdc.decode(bytes.get)
    assert(newObj.isEmpty)
  }


  "A KryoCodecSpec instance" should "codec simple object with bytes" in assertCodec(shoe, Codec.spawn[Shoe](1, Codec.Kryo, LgScala))

  it should "codec composite object with bytes" in assertCodec(gary, Codec.spawn[Person](1, Codec.Kryo, LgScala))

  it should "codec Array consist of simple object with bytes" in assertCodec(Array(shoe), Codec.spawn[Array[Shoe]](1, Codec.Kryo, LgScala))

  it should "codec Array consist of composite object with bytes" in assertCodec(Array(jack), Codec.spawn[Array[Person]](1, Codec.Kryo, LgScala))

  it should "codec Seq consist of composite object with bytes" in assertCodec(List(jack), Codec.spawn[List[Person]](1, Codec.Kryo, LgScala))

  // not support scala set cause Codec's type parameter can't be Set$Set1 or Set$Set2 etc
  // the type parameter is just the Set trait so it can't be instantiate
  it should "codec Set consist of composite object with bytes" in {
    val obj = Set(jack)
    val cdc = Codec.spawn[Set[Person]](1, Codec.Kryo, LgScala)
    val bytes = cdc.encode(obj)
    assert(bytes.isDefined)

    val newObj = cdc.decode(bytes.get)
    assert(newObj.isEmpty)
  }

  it should "codec composite object contains Seq field with bytes" in assertCodec(groupSeq, Codec.spawn[GroupBySeq](1, Codec.Kryo, LgScala))

  it should "codec composite object contains Map field with bytes" in assertCodec(groupMap, Codec.spawn[GroupByMap](1, Codec.Kryo, LgScala))

  it should "codec composite object contains Set field with bytes" in assertCodec(groupSet, Codec.spawn[GroupBySet](1, Codec.Kryo, LgScala))



  it should "codec java List consist of scala simple object with bytes" in {
    val jlist = new JArrayList[Shoe]()
    jlist.add(shoe)
    assertCodec[JArrayList[Shoe]](jlist, Codec.spawn[JArrayList[Shoe]](1, Codec.Kryo, LgJava))
  }

  it should "codec java Set consist of scala simple object with bytes" in {
    val jset = new JHashSet[Shoe]()
    jset.add(shoe)
    assertCodec[JHashSet[Shoe]](jset, Codec.spawn[JHashSet[Shoe]](1, Codec.Kryo, LgJava))
  }

  it should "codec java Map consist of scala simple object with bytes" in {
    val jmap = new JHashMap[Int, Shoe]()
    jmap.put(1, shoe)
    assertCodec(jmap, Codec.spawn[JHashMap[Int, Shoe]](1, Codec.Kryo, LgJava))
  }

  it should "codec simple java bean with bytes" in {
    val cdc = Codec.spawn[Shirt](1, Codec.Kryo, LgJava)
    val bytes = cdc.encode(shirtA)
    assert(bytes.isDefined)

    val newObj = cdc.decode(bytes.get)
    assert(newObj.isDefined)
    assert(newObj.get.getColor == shirtA.getColor)
  }

  it should "codec composite java bean with bytes" in {
    val cdc = Codec.spawn[Shop](1, Codec.Kryo, LgJava)
    val bytes = cdc.encode(shop)
    assert(bytes.isDefined)

    val newObj = cdc.decode(bytes.get)
    assert(newObj.isDefined)
    assert(newObj.get.getName == shop.getName)
    assert(newObj.get.getProductions.get(0).getColor == shop.getProductions.get(0).getColor)
  }





  //noinspection CorrespondsUnsorted
  def assertCodec[T <: AnyRef](obj: T, cdc: Codec[T]) = {
    val bytes = cdc.encode(obj)
    assert(bytes.isDefined)
//    println(bytes.get.length)
//    println(new String(bytes.get))

    val newObj = cdc.decode(bytes.get)
    assert(newObj.isDefined)
    obj match {
      case ooo: Array[_] => assert(ooo sameElements newObj.get.asInstanceOf[Array[_]])
      case _ => assert(newObj.get == obj)
    }
  }

}
