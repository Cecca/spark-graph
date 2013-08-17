package it.unipd.dei.graph

trait Timed {

  def timed[R](name: String)(f: => R): R = {
    val start = System.currentTimeMillis()
    val ret = f
    val end = System.currentTimeMillis()
    println("%s time: %d ms".format(name, (end-start)))
    ret
  }

}
