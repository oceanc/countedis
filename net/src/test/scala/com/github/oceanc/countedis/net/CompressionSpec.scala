package com.github.oceanc.countedis.net

import io.netty.util.CharsetUtil

/**
 * @author chengyang
 */
class CompressionSpec extends UnitSpec {
  val json = """{"name":"CocaCola","owner":"tom","productions":[{"color":"blue","logo":"adidas","weight":20,"produceDate":"2015-11-19 13:55:25.512"},{"color":"blue","logo":"adidas","weight":20,"produceDate":"2016-11-19 13:55:25.512"},{"color":"blue","logo":"adidas","weight":20,"produceDate":"2015-11-19 13:55:25.512"}]}"""
  val jsonBytes = json.getBytes(CharsetUtil.UTF_8)

  "A LZ4 compression" should "compression json bytes" in {
    val compressedBytes = Compression.compress(jsonBytes)(Lz4Compress)
    assert(compressedBytes.isDefined)
    assert(compressedBytes.get.length < jsonBytes.length)
    println(jsonBytes.length)
    println(compressedBytes.get.length)

    val newJsonBytes = Compression.decompress(compressedBytes.get, jsonBytes.length)(Lz4Compress)
    assert(newJsonBytes.isDefined)
    assert(json == new String(newJsonBytes.get))
  }

  it should "return [0] when compress empty array" in {
    val compressedBytes = Compression.compress(Array.empty[Byte])(Lz4Compress)
    assert(compressedBytes.isDefined)
    assert(compressedBytes.get(0) == 0)

    val newJsonBytes = Compression.decompress(compressedBytes.get, 0)(Lz4Compress)
    assert(newJsonBytes.isDefined)
    assert(0 == newJsonBytes.get.length)
  }

  it should "return None when decompress array has error length" in {
    val compressedBytes = Compression.compress(jsonBytes)(Lz4Compress)
    assert(compressedBytes.isDefined)
    assert(compressedBytes.get.length < jsonBytes.length)

    val newJsonBytes = Compression.decompress(compressedBytes.get, jsonBytes.length - 4)(Lz4Compress)
    assert(newJsonBytes.isEmpty)
  }

  "A Snappy compression" should "compression json bytes" in {
    val compressedBytes = Compression.compress(jsonBytes)(SnappyCompress)
    assert(compressedBytes.isDefined)
    assert(compressedBytes.get.length < jsonBytes.length)
    println(jsonBytes.length)
    println(compressedBytes.get.length)

    val newJsonBytes = Compression.decompress(compressedBytes.get, jsonBytes.length)(SnappyCompress)
    assert(newJsonBytes.isDefined)
    assert(json == new String(newJsonBytes.get))
  }

  it should "return [0] when compress empty array" in {
    val compressedBytes = Compression.compress(Array.empty[Byte])(SnappyCompress)
    assert(compressedBytes.isDefined)
    assert(compressedBytes.get(0) == 0)

    val newJsonBytes = Compression.decompress(compressedBytes.get, 0)(SnappyCompress)
    assert(newJsonBytes.isDefined)
    assert(0 == newJsonBytes.get.length)
  }

  it should "return new Bytes even if decompress array has error length" in {
    val compressedBytes = Compression.compress(jsonBytes)(SnappyCompress)
    assert(compressedBytes.isDefined)
    assert(compressedBytes.get.length < jsonBytes.length)

    val newJsonBytes = Compression.decompress(compressedBytes.get, jsonBytes.length - 4)(SnappyCompress)
    assert(newJsonBytes.isDefined)
  }

}
