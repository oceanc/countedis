package com.github.oceanc.countedis.net.benckmark

/**
 * @author chengyang
 */
trait Benchmark {
  def loopRun(c: Int, f: => Unit) = {
    var i = 0
    while (i < c) {
      i += 1
      f
    }
  }
}
