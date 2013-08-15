package it.unipd.dei

trait Timed {

  def timed[R](name: String)(f: => R): R = {
    val start = System.currentTimeMillis()
    val ret = f
    val end = System.currentTimeMillis()
    println(name +": " + (end-start) + "ms")
    ret
  }

}
